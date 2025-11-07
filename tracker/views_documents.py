import json
import os
import logging
from decimal import Decimal
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.db import transaction
from django.shortcuts import get_object_or_404

from .models import DocumentScan, DocumentExtraction, Order, Vehicle, Customer, Branch, Invoice, InvoiceLineItem
from .utils.document_extraction import DocumentExtractor, extract_document, match_document_to_records
from .extraction_utils import process_invoice_extraction
from .utils import get_user_branch

logger = logging.getLogger(__name__)


@login_required
@require_http_methods(["POST"])
def upload_document(request):
    """Upload a document, optionally attach to an existing order, and start async extraction.

    Accepts FormData with:
    - file (required)
    - vehicle_plate (optional)
    - customer_phone (optional)
    - document_type (optional, defaults to 'invoice')
    - order_id (optional) — attach the upload to this order

    Returns immediately with document_id and extraction_id.
    Extraction happens in background. Use get_document_status to poll progress.
    """
    try:
        file = request.FILES.get('file')
        vehicle_plate = request.POST.get('vehicle_plate', '').strip()
        customer_phone = request.POST.get('customer_phone', '').strip()
        document_type = request.POST.get('document_type', 'invoice')
        order_id = request.POST.get('order_id')

        if not file:
            return JsonResponse({'success': False, 'error': 'No file uploaded'}, status=400)

        user_branch = get_user_branch(request.user)
        order = None
        if order_id:
            try:
                # Fetch the order without filtering by branch first
                order = Order.objects.get(id=int(order_id))
                # If the user has a branch assigned and it conflicts with the order branch, block
                if user_branch and order.branch and order.branch != user_branch:
                    return JsonResponse({'success': False, 'error': 'Order belongs to a different branch'}, status=403)
            except Exception:
                order = None

        with transaction.atomic():
            doc_scan = DocumentScan.objects.create(
                order=order,
                vehicle_plate=vehicle_plate or (order.vehicle.plate_number if order and order.vehicle else ''),
                customer_phone=customer_phone or (order.customer.phone if order and order.customer else ''),
                file=file,
                document_type=document_type,
                uploaded_by=request.user,
                file_name=file.name,
                file_size=file.size,
                file_mime_type=file.content_type,
                extraction_status='pending'
            )

        # Start async extraction in background thread
        from .utils.async_extraction import start_extraction_task
        start_extraction_task(doc_scan.id)

        # Return immediately with document info
        return JsonResponse({
            'success': True,
            'document_id': doc_scan.id,
            'extraction_id': None,  # Will be populated after extraction completes
            'status': 'pending',
            'message': 'Invoice uploaded and queued for processing'
        })

    except Exception as e:
        logger.error(f"Error uploading document: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def _perform_extraction(doc_scan: DocumentScan):
    """Document extraction has been removed."""
    raise NotImplementedError('Document extraction removed')


@login_required
@require_http_methods(["GET"])
def get_document_status(request, doc_id):
    """Get extraction status and progress for a document.

    Returns:
        {
            'success': bool,
            'status': 'pending|processing|completed|failed',
            'progress': 0-100,
            'message': 'status message',
            'document_id': id,
            'extraction_id': id or null,
            'extracted_data': {} or null,
            'confidence': percentage or null,
            'matches': {} or null
        }
    """
    try:
        user_branch = get_user_branch(request.user)
        doc_scan = get_object_or_404(DocumentScan, id=int(doc_id))

        # Check authorization - allow same-branch users OR the original uploader
        if doc_scan.order and doc_scan.order.branch != user_branch:
            if doc_scan.uploaded_by != request.user:
                return JsonResponse({'success': False, 'error': 'Unauthorized'}, status=403)

        from .utils.async_extraction import get_extraction_progress
        progress_info = get_extraction_progress(doc_scan.id)

        # If extraction completed, get the extracted data
        extracted_data = None
        extraction_id = None
        confidence = None
        matches = None

        if progress_info['status'] == 'completed':
            try:
                extraction = DocumentExtraction.objects.get(document=doc_scan)
                extraction_id = extraction.id
                confidence = extraction.confidence_overall
                extracted_data = extraction.extracted_data_json

                # Try to build matches
                if extracted_data and extracted_data.get('plate_number'):
                    v = Vehicle.objects.filter(
                        plate_number__iexact=extracted_data.get('plate_number'),
                        customer__branch=user_branch
                    ).select_related('customer').first()
                    if v:
                        matches = {
                            'vehicle': {'id': v.id, 'plate': v.plate_number, 'make': v.make, 'model': v.model},
                            'customer': {'id': v.customer.id, 'name': v.customer.full_name, 'phone': v.customer.phone}
                        }
            except DocumentExtraction.DoesNotExist:
                pass

        return JsonResponse({
            'success': True,
            'status': progress_info['status'],
            'progress': progress_info['progress'],
            'message': progress_info['message'],
            'document_id': doc_scan.id,
            'extraction_id': extraction_id,
            'extracted_data': extracted_data,
            'confidence': confidence,
            'matches': matches or {}
        })
    except Exception as e:
        logger.error(f"Error getting document status: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
@require_http_methods(["GET"])
def get_document_extraction(request, doc_id):
    return JsonResponse({'success': False, 'error': 'Document extraction retrieval disabled'}, status=410)


@login_required
@require_http_methods(["POST"])
def create_order_from_document(request):
    """Create a new order from an existing extraction/document.

    Body JSON:
    {
        "extraction_id": 123,            // preferred
        "document_id": 456,              // fallback if extraction_id missing
        "vehicle_plate": "T 123 ABC",   // optional hints
        "customer_phone": "+255...",    // optional
        "use_extracted": true            // default true
    }
    """
    try:
        data = json.loads(request.body or '{}')
        extraction_id = data.get('extraction_id')
        document_id = data.get('document_id')
        use_extracted = data.get('use_extracted', True)

        # Resolve extraction
        extraction = None
        if extraction_id:
            extraction = get_object_or_404(DocumentExtraction, id=int(extraction_id))
        elif document_id:
            extraction = DocumentExtraction.objects.filter(document_id=int(document_id)).first()
            if not extraction:
                return JsonResponse({'success': False, 'error': 'No extraction found for this document'}, status=404)
        else:
            return JsonResponse({'success': False, 'error': 'extraction_id or document_id is required'}, status=400)

        user_branch = get_user_branch(request.user)
        if not user_branch:
            # allow creating without a branch; branch field may be nullable depending on model
            pass

        from .services import CustomerService, VehicleService

        extracted = extraction.extracted_data_json or {}

        # Extract customer data
        cust_name = extraction.extracted_customer_name or extracted.get('customer_name') or 'Customer'
        cust_phone = extraction.extracted_customer_phone or extracted.get('customer_phone') or data.get('customer_phone') or ''
        cust_email = extraction.extracted_customer_email or extracted.get('customer_email') or ''
        cust_addr = getattr(extraction, 'extracted_customer_address', None) or extracted.get('address') or ''

        # Create or get customer using centralized service
        try:
            customer, _ = CustomerService.create_or_get_customer(
                branch=user_branch,
                full_name=cust_name,
                phone=cust_phone,
                email=cust_email or None,
                address=cust_addr or None,
                customer_type='personal'
            )
        except Exception as e:
            logger.warning(f"Failed to create customer from extraction: {e}")
            customer = Customer.objects.create(
                branch=user_branch,
                full_name=cust_name,
                phone=cust_phone,
                email=cust_email or None,
                address=cust_addr or None,
                customer_type='personal'
            )

        # Vehicle
        extracted_plate = (
            extraction.extracted_vehicle_plate
            or extracted.get('plate_number')
            or data.get('vehicle_plate')
            or ''
        ).strip()

        vehicle = None
        if extracted_plate:
            vehicle = VehicleService.create_or_get_vehicle(
                customer=customer,
                plate_number=extracted_plate,
                make=extracted.get('vehicle_make'),
                model=extracted.get('vehicle_model')
            )

        # Description and estimation
        description = extracted.get('service_description') or extraction.extracted_order_description or ''
        if extracted.get('matched_service'):
            description = (description + ('; ' if description else '') + extracted.get('matched_service')).strip()

        est_minutes = extracted.get('estimated_minutes') or 0

        # Create order
        order = Order.objects.create(
            customer=customer,
            vehicle=vehicle,
            branch=user_branch,
            type='service',
            status='created',
            description=description or None,
            estimated_duration=est_minutes or None,
        )
        order.started_at = timezone.now()
        order.save(update_fields=['started_at'])

        return JsonResponse({
            'success': True,
            'order_id': order.id,
            'order_number': order.order_number,
            'customer_id': customer.id,
            'vehicle_id': vehicle.id if vehicle else None
        })
    except Exception as e:
        logger.error(f"Error creating order from document: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
@require_http_methods(["POST"])
def verify_and_update_extraction(request):
    return JsonResponse({'success': False, 'error': 'Verification of extraction disabled'}, status=410)


@login_required
@require_http_methods(["POST"])
def search_by_job_card(request):
    return JsonResponse({'success': False, 'error': 'Search by job card disabled'}, status=410)
@login_required
@require_http_methods(["GET"])
def api_get_extraction(request):
    """API endpoint to get extraction data for a document"""
    try:
        extraction_id = request.GET.get('extraction_id')
        if not extraction_id:
            return JsonResponse({'success': False, 'error': 'extraction_id required'}, status=400)

        extraction = get_object_or_404(DocumentExtraction, id=int(extraction_id))

        return JsonResponse({
            'success': True,
            'extraction': {
                'id': extraction.id,
                'extracted_customer_name': extraction.extracted_customer_name,
                'extracted_customer_phone': extraction.extracted_customer_phone,
                'extracted_customer_email': extraction.extracted_customer_email,
                'extracted_vehicle_plate': extraction.extracted_vehicle_plate,
                'extracted_vehicle_make': extraction.extracted_vehicle_make,
                'extracted_vehicle_model': extraction.extracted_vehicle_model,
                'extracted_order_description': extraction.extracted_order_description,
                'extracted_item_name': extraction.extracted_item_name,
                'extracted_brand': extraction.extracted_brand,
                'extracted_quantity': extraction.extracted_quantity,
                'extracted_amount': extraction.extracted_amount,
                'code_no': extraction.code_no,
                'reference': extraction.reference,
                'net_value': str(extraction.net_value) if extraction.net_value else None,
                'vat_amount': str(extraction.vat_amount) if extraction.vat_amount else None,
                'gross_value': str(extraction.gross_value) if extraction.gross_value else None,
                'confidence_overall': extraction.confidence_overall,
            }
        })
    except Exception as e:
        logger.error(f"Error getting extraction: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
@require_http_methods(["POST"])
def api_create_invoice_from_extraction(request):
    """API endpoint to create an invoice from extracted data"""
    try:
        data = json.loads(request.body or '{}')
        extraction_id = data.get('extraction_id')
        order_id = data.get('order_id')

        if not extraction_id:
            return JsonResponse({'success': False, 'error': 'extraction_id required'}, status=400)

        extraction = get_object_or_404(DocumentExtraction, id=int(extraction_id))
        user_branch = get_user_branch(request.user)

        # Get or create customer from extraction
        from .services import CustomerService, VehicleService

        cust_name = extraction.extracted_customer_name or 'Customer'
        cust_phone = extraction.extracted_customer_phone or ''
        cust_email = extraction.extracted_customer_email or None
        cust_addr = extraction.extracted_customer_address or None

        try:
            customer, _ = CustomerService.create_or_get_customer(
                branch=user_branch,
                full_name=cust_name,
                phone=cust_phone,
                email=cust_email,
                address=cust_addr,
                customer_type='personal'
            )
        except Exception as e:
            logger.warning(f"Failed to create customer: {e}")
            customer = Customer.objects.create(
                branch=user_branch,
                full_name=cust_name,
                phone=cust_phone,
                email=cust_email,
                address=cust_addr,
                customer_type='personal'
            )

        # Get or create vehicle from extraction
        vehicle = None
        if extraction.extracted_vehicle_plate:
            try:
                vehicle = VehicleService.create_or_get_vehicle(
                    customer=customer,
                    plate_number=extraction.extracted_vehicle_plate,
                    make=extraction.extracted_vehicle_make or '',
                    model=extraction.extracted_vehicle_model or '',
                    vehicle_type=extraction.extracted_data_json.get('vehicle_type') if extraction.extracted_data_json else ''
                )
            except Exception as e:
                logger.warning(f"Failed to create vehicle: {e}")

        # Get existing order if provided, or try to find one by plate
        order = None
        if order_id:
            try:
                order = Order.objects.get(id=order_id, branch=user_branch)
            except Order.DoesNotExist:
                pass

        # If no order provided, try to find an existing started order by plate
        if not order and vehicle:
            order = Order.objects.filter(
                vehicle=vehicle,
                status='created'
            ).order_by('-created_at').first()

        # If still no order, try to find by extracted plate
        if not order and extraction.extracted_vehicle_plate:
            existing_vehicle = Vehicle.objects.filter(
                plate_number__iexact=extraction.extracted_vehicle_plate,
                customer__branch=user_branch
            ).select_related('customer').first()
            if existing_vehicle:
                order = Order.objects.filter(
                    vehicle=existing_vehicle,
                    status='created'
                ).order_by('-created_at').first()

        # Determine invoice date: prefer extracted date, then order start time, then current date
        invoice_date = timezone.now().date()
        if extraction.invoice_date:
            invoice_date = extraction.invoice_date
        elif order and order.started_at:
            invoice_date = order.started_at.date()

        # Create invoice
        with transaction.atomic():
            invoice = Invoice.objects.create(
                branch=user_branch,
                order=order,
                customer=customer,
                vehicle=vehicle,
                reference=data.get('reference') or extraction.reference or extraction.extracted_vehicle_plate or '',
                invoice_date=invoice_date,
                due_date=data.get('due_date') or extraction.del_date or None,
                tax_rate=data.get('tax_rate') or 0,
                attended_by=data.get('attended_by') or extraction.attended_by or '',
                kind_attention=data.get('kind_attention') or '',
                notes=data.get('notes') or '',
                terms=data.get('terms') or (
                    "NOTE 1 : Payment in TSHS accepted at the prevailing rate on the date of payment. "
                    "2 : Proforma Invoice is Valid for 2 weeks from date of Proforma. "
                    "3 : Discount is Valid only for the above Quantity. "
                    "4 : Duty and VAT exemption documents to be submitted with the Purchase Order."
                ),
                created_by=request.user,
            )
            invoice.generate_invoice_number()
            invoice.save()

            # Create line items from extracted items
            try:
                items = extraction.items.all()
                if items:
                    for item in items:
                        unit_price = item.rate or Decimal('0')
                        quantity = item.qty or Decimal('1')

                        # Determine tax rate: use item VAT if available, else invoice rate
                        item_tax_rate = Decimal('0')
                        if item.vat and item.value:
                            # Calculate tax rate from VAT amount: tax_rate = (vat / value) * 100
                            try:
                                item_tax_rate = (item.vat / item.value) * 100
                            except Exception:
                                item_tax_rate = invoice.tax_rate or Decimal('0')
                        else:
                            item_tax_rate = invoice.tax_rate or Decimal('0')

                        InvoiceLineItem.objects.create(
                            invoice=invoice,
                            code=item.code or '',
                            description=item.description or '',
                            item_type='custom',
                            quantity=quantity,
                            unit=item.unit or 'PCS',
                            unit_price=unit_price,
                            tax_rate=item_tax_rate,
                        )
                elif extraction.extracted_amount or extraction.gross_value:
                    # Create a single line item if we have amount but no itemized details
                    amount = extraction.gross_value or extraction.extracted_amount or 0
                    try:
                        amount_decimal = Decimal(str(amount))
                    except Exception:
                        amount_decimal = Decimal('0')

                    InvoiceLineItem.objects.create(
                        invoice=invoice,
                        description=extraction.extracted_order_description or 'Service',
                        item_type='custom',
                        quantity=1,
                        unit='Unit',
                        unit_price=amount_decimal,
                        tax_rate=invoice.tax_rate,
                    )
            except Exception as e:
                logger.warning(f"Failed to create line items: {e}")

        return JsonResponse({
            'success': True,
            'invoice_id': invoice.id,
            'invoice_number': invoice.invoice_number,
            'message': 'Invoice created successfully'
        })

    except Exception as e:
        logger.error(f"Error creating invoice from extraction: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
@require_http_methods(["POST"])
def start_quick_order(request):
    """Start a quick order with job card number, to be filled later with document"""
    try:
        data = json.loads(request.body)
        job_card_number = data.get('job_card_number', '').strip()
        vehicle_plate = data.get('vehicle_plate', '').strip()
        
        if not job_card_number:
            return JsonResponse({
                'success': False,
                'error': 'Job card number is required'
            }, status=400)
        
        user_branch = get_user_branch(request.user)
        
        # Check if order already exists
        existing_order = Order.objects.filter(
            job_card_number=job_card_number,
            branch=user_branch
        ).first()
        
        if existing_order:
            return JsonResponse({
                'success': False,
                'error': 'Order with this job card already exists',
                'order_id': existing_order.id,
                'order_number': existing_order.order_number,
            }, status=400)
        
        from .services import VehicleService

        # Find existing customer by vehicle plate if provided
        customer = None
        vehicle = None

        if vehicle_plate:
            vehicle = Vehicle.objects.filter(
                plate_number__iexact=vehicle_plate,
                customer__branch=user_branch
            ).first()

            if vehicle:
                customer = vehicle.customer

        # Don't create a temp customer - wait for document extraction to get real customer data
        # This prevents the "Pending - T XXX" problem
        if not customer:
            # Create a placeholder that will be replaced when document is processed
            from .services import CustomerService
            customer, _ = CustomerService.create_or_get_customer(
                branch=user_branch,
                full_name=f"Job Card {job_card_number}",
                phone=f"JC{job_card_number}",  # Use job card as temp identifier instead of "pending"
                customer_type='personal',
            )
        
        # Create order
        order = Order.objects.create(
            customer=customer,
            vehicle=vehicle,
            branch=user_branch,
            type='service',
            status='created',
            job_card_number=job_card_number,
            description=f"Order started with job card {job_card_number}",
        )
        
        # Set start time
        order.started_at = timezone.now()
        order.save(update_fields=['started_at'])
        
        return JsonResponse({
            'success': True,
            'order_id': order.id,
            'order_number': order.order_number,
            'job_card_number': order.job_card_number,
            'message': 'Quick order started. Upload document to fill in details.'
        })
    
    except Exception as e:
        logger.error(f"Error starting quick order: {str(e)}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)
