"""
Invoice extraction utilities using template-based pattern matching.
Provides functions to extract customer, vehicle, service, and financial data from invoice text.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple
from decimal import Decimal

logger = logging.getLogger(__name__)


class InvoiceExtractor:
    """Template-based invoice field extractor using regex patterns."""
    
    def __init__(self):
        """Initialize the extractor (patterns loaded from database on first use)."""
        self.patterns = {}
        self.service_templates = {}
        self._patterns_loaded = False
    
    def _load_patterns_from_db(self):
        """Load extraction patterns from database."""
        if self._patterns_loaded:
            return
        
        try:
            from .models import InvoicePatternMatcher, ServiceTemplate
            
            patterns = InvoicePatternMatcher.objects.filter(is_active=True).order_by('priority')
            for pattern in patterns:
                field_type = pattern.field_type
                if field_type not in self.patterns:
                    self.patterns[field_type] = []
                self.patterns[field_type].append({
                    'name': pattern.name,
                    'regex': pattern.regex_pattern,
                    'group': pattern.extract_group,
                    'priority': pattern.priority,
                })
            
            # Load service templates for keyword matching
            templates = ServiceTemplate.objects.filter(is_active=True)
            for template in templates:
                keywords = [k.strip().lower() for k in (template.keywords or '').split(',') if k.strip()]
                self.service_templates[template.name] = {
                    'keywords': keywords,
                    'minutes': template.estimated_minutes,
                    'service_type': template.service_type,
                }
            
            self._patterns_loaded = True
        except Exception as e:
            logger.error(f"Error loading patterns from database: {str(e)}")
            self._patterns_loaded = True  # Prevent repeated attempts
    
    def _default_patterns(self) -> Dict:
        """Return default patterns if database patterns are unavailable."""
        return {
            'code_no': [
                {
                    'name': 'Code No label',
                    'regex': r'(?mi)^\s*Code\s*No\.?\s*[:\-]?\s*([A-Z0-9\-/]+)\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
            'pi_no': [
                {
                    'name': 'PI No label',
                    'regex': r'(?mi)^\s*(?:PI\s*No\.?|P\.I\.\s*No\.?|Proforma\s*(?:Invoice\s*)?No\.?)\s*[:\-]?\s*([A-Z0-9\-/]+)\s*$',
                    'group': 1,
                    'priority': 5,
                },
                {
                    'name': 'Invoice number from header',
                    'regex': r'(?:Proforma\s+Invoice|Invoice)\s*(?:No\.?|#)\s*[:\-]?\s*([A-Z0-9\-/]+)',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'invoice_date': [
                {
                    'name': 'Date label',
                    'regex': r'(?mi)^\s*Date\s*[:\-]?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
            'del_date': [
                {
                    'name': 'Delivery Date label',
                    'regex': r'(?mi)^\s*Del\.?\s*Date\s*[:\-]?\s*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
            'customer_tel': [
                {
                    'name': 'Tel label exact',
                    'regex': r'(?mi)^\s*Tel\s*[:\-]?\s*(\+?255\s?\d{3}\s?\d{3}\s?\d{3}|0[67]\d{2}\s?\d{3}\s?\d{3}|\+?\d{7,15})\s*$',
                    'group': 1,
                    'priority': 5,
                },
                {
                    'name': 'Tel label TZ formats',
                    'regex': r'(?:Tel|Telephone|Phone)[\s:]*(\+?255\s?\d{3}\s?\d{3}\s?\d{3}|0[67]\d{2}\s?\d{3}\s?\d{3}|\+?\d{7,15})',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'attended_by': [
                {
                    'name': 'Attended by label',
                    'regex': r'(?mi)^\s*Attended\s*(?:by|By)\s*[:\-]?\s*([^\n]+?)\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
            'plate_number': [
                {
                    'name': 'Plate in reference field',
                    'regex': r'(?:REFERENCE|REF|Reference|Ref|Plate|License)[\s:]*([A-Z]{1,3}\s?\d{1,5}\s?[A-Z]{2,3}|[A-Z]{2,3}\s?[A-Z]?\s?\d+\s?[A-Z]{2,3})',
                    'group': 1,
                    'priority': 10,
                },
                {
                    'name': 'Standard plate format',
                    'regex': r'(?<![A-Z0-9])([A-Z]{1,3}\s?\d{1,5}\s?[A-Z]{0,3})(?![A-Z0-9])',
                    'group': 1,
                    'priority': 20,
                },
            ],
            'amount': [
                {
                    'name': 'Gross Value block',
                    'regex': r'(?mi)^(?:\s*Gross\s*Value\s*[:\s]*)\s*([\d,]+\.?\d{0,2})\s*$',
                    'group': 1,
                    'priority': 5,
                },
                {
                    'name': 'Net Value block',
                    'regex': r'(?mi)^(?:\s*Net\s*Value\s*[:\s]*)\s*([\d,]+\.?\d{0,2})\s*$',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'customer_phone': [
                {
                    'name': 'Tel label TZ formats',
                    'regex': r'(?mi)^\s*(?:Tel|Telephone|Phone)\s*[:\-]?\s*(\+?255\s?\d{3}\s?\d{3}\s?\d{3}|0[67]\d{2}\s?\d{3}\s?\d{3})',
                    'group': 1,
                    'priority': 10,
                },
                {
                    'name': 'General phone format',
                    'regex': r'(\+?\d{9,15})',
                    'group': 1,
                    'priority': 20,
                },
            ],
            'customer_name': [
                {
                    'name': 'Strict Customer Name label',
                    'regex': r'(?mi)^\s*Customer\s*Name\s*[:\-]?\s*([^\n]+?)\s*$',
                    'group': 1,
                    'priority': 1,
                },
                {
                    'name': 'Fallback CUSTOMER NAME (caps)',
                    'regex': r'(?mi)^\s*CUSTOMER\s*NAME\s*[:\-]?\s*([^\n]+?)\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
            'address': [
                {
                    'name': 'Address block',
                    'regex': r'(?mis)\bAddress\b\s*[:\-]?\s*([^\n]+?(?:\n[^\n]+)*)?(?=\n\s*(?:Tel|Telephone|Attended|Reference|PI\s*No\.|Code\s*No|Fax)\b)',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'customer_email': [
                {
                    'name': 'Email pattern',
                    'regex': r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'service_description': [
                {
                    'name': 'Service/description field',
                    'regex': r'(?:SERVICE|Service|Description|Item|ITEM)[\s:]*([A-Za-z0-9\s,./\\-]+?)(?:\n|Qty|Quantity|QTY|$)',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'quantity': [
                {
                    'name': 'Quantity field',
                    'regex': r'(?:QTY|Quantity|Qty)[\s:]*(\d+)',
                    'group': 1,
                    'priority': 10,
                },
            ],
            'reference': [
                {
                    'name': 'Reference labeled full',
                    'regex': r'(?mi)^\s*Reference\s*[:\-]?\s*([A-Z0-9\s\-/]+?)\s*$',
                    'group': 1,
                    'priority': 1,
                },
                {
                    'name': 'Ref short',
                    'regex': r'(?mi)^\s*Ref\s*(?:No\.|:)?\s*([A-Z0-9\s\-/]+?)\s*$',
                    'group': 1,
                    'priority': 5,
                },
            ],
        }
    
    def extract_field(self, text: str, field_type: str) -> Optional[str]:
        """
        Extract a specific field from invoice text using regex patterns.
        
        Args:
            text: Raw invoice text
            field_type: Type of field to extract (e.g., 'plate_number', 'amount')
        
        Returns:
            Extracted value or None
        """
        self._load_patterns_from_db()
        
        patterns = self.patterns.get(field_type)
        if not patterns:
            patterns = self._default_patterns().get(field_type, [])
        
        for pattern_info in patterns:
            try:
                match = re.search(pattern_info['regex'], text, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = match.group(pattern_info.get('group', 1))
                    if value:
                        return value.strip()
            except Exception as e:
                logger.warning(f"Error matching pattern {pattern_info['name']}: {str(e)}")
        
        return None
    
    def extract_amount(self, text: str) -> Optional[Decimal]:
        """Extract and parse monetary amount from text."""
        amount_str = self.extract_field(text, 'amount')
        if not amount_str:
            return None

        try:
            # Remove non-numeric characters except decimal point
            amount_str = re.sub(r'[^\d.]', '', amount_str)
            return Decimal(amount_str)
        except Exception as e:
            logger.warning(f"Error parsing amount '{amount_str}': {str(e)}")
            return None

    def extract_pi_no(self, text: str) -> Optional[str]:
        """Extract Proforma Invoice Number (PI No)."""
        return self.extract_field(text, 'pi_no')

    def extract_invoice_date(self, text: str) -> Optional[str]:
        """Extract invoice date from text."""
        return self.extract_field(text, 'invoice_date')

    def extract_del_date(self, text: str) -> Optional[str]:
        """Extract delivery date from text."""
        return self.extract_field(text, 'del_date')

    def extract_customer_tel(self, text: str) -> Optional[str]:
        """Extract customer telephone from text."""
        return self.extract_field(text, 'customer_tel')

    def extract_attended_by(self, text: str) -> Optional[str]:
        """Extract attended by field from text."""
        return self.extract_field(text, 'attended_by')
    
    def match_service_template(self, description: str) -> Optional[Tuple[str, int]]:
        """
        Match a service description to a template and return estimated minutes.
        
        Args:
            description: Service description text
        
        Returns:
            Tuple of (service_name, estimated_minutes) or None
        """
        if not description:
            return None
        
        description_lower = description.lower()
        
        # Find best matching template based on keywords
        best_match = None
        best_match_count = 0
        
        for service_name, template in self.service_templates.items():
            match_count = sum(1 for kw in template['keywords'] if kw in description_lower)
            if match_count > best_match_count:
                best_match = (service_name, template['minutes'])
                best_match_count = match_count
        
        return best_match
    
    def extract_all(self, text: str) -> Dict:
        """
        Extract all available fields from invoice text.

        Args:
            text: Raw invoice text

        Returns:
            Dictionary with extracted fields
        """
        self._load_patterns_from_db()

        extracted = {
            'plate_number': self.extract_field(text, 'plate_number'),
            'customer_name': self.extract_field(text, 'customer_name'),
            'customer_phone': self.extract_field(text, 'customer_phone'),
            'customer_email': self.extract_field(text, 'customer_email'),
            'customer_tel': self.extract_customer_tel(text),
            'address': self.extract_field(text, 'address'),
            'service_description': self.extract_field(text, 'service_description'),
            'item_name': self.extract_field(text, 'service_description'),
            'quantity': self.extract_field(text, 'quantity'),
            'amount': str(self.extract_amount(text)) if self.extract_amount(text) else None,
            'reference': self.extract_field(text, 'reference'),
            'code_no': self.extract_field(text, 'code_no'),
            'customer_code': self.extract_field(text, 'code_no'),
            'pi_no': self.extract_pi_no(text),
            'invoice_date': self.extract_invoice_date(text),
            'del_date': self.extract_del_date(text),
            'attended_by': self.extract_attended_by(text),
        }

        # Fallback: some layouts put value on the next line after the label
        if not extracted['customer_name']:
            try:
                lines = [ln.strip() for ln in text.split('\n')]
                for i, ln in enumerate(lines):
                    if re.match(r'(?i)^customer\s*name\s*[:\-]?\s*$', ln):
                        # take the next non-empty line that isn't another label
                        j = i + 1
                        while j < len(lines) and not lines[j].strip():
                            j += 1
                        if j < len(lines):
                            nxt = lines[j].strip()
                            if not re.match(r'(?i)^(address|tel|telephone|fax|attended|kind\s*attn|ref|reference|pi\s*no\.|date)\b', nxt):
                                extracted['customer_name'] = nxt
                                break
            except Exception:
                pass
        
        # Try to match service template if we have a service description
        if extracted['service_description']:
            match = self.match_service_template(extracted['service_description'])
            if match:
                extracted['matched_service'] = match[0]
                extracted['estimated_minutes'] = match[1]
        
        # Remove None values
        return {k: v for k, v in extracted.items() if v is not None}


def extract_text_from_image(image_path: str) -> str:
    """
    Image text extraction is disabled (OCR not used). This function will
    return an empty string and log a warning. If you need text extraction
    from images, enable an OCR solution or provide PDF/text documents.
    """
    logger.warning('OCR disabled: extract_text_from_image called but OCR is not enabled')
    return ""


def process_invoice_extraction(document_scan) -> Dict:
    """
    Process a document scan and extract all available data.
    
    Args:
        document_scan: DocumentScan instance with uploaded file
    
    Returns:
        Dictionary of extracted data
    """
    extractor = InvoiceExtractor()
    text = ""
    
    try:
        # Try to extract text from file
        if document_scan.file.name.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.bmp')):
            # OCR disabled — instruct user to upload a PDF or text-based document
            logger.warning('Image upload detected but OCR is disabled — extraction aborted')
            return {'error': 'Image extraction disabled. Please upload a PDF or text-based document.'}
        else:
            # If it's a PDF, attempt binary-based extraction
            try:
                # Prefer using PDF extraction utilities instead of raw decode
                from tracker.utils.document_extraction import DocumentExtractor
                dext = DocumentExtractor()
                # Save uploaded file to a temporary path and extract
                tmp_path = None
                try:
                    tmp_path = document_scan.file.path
                except Exception:
                    # Fallback: write content to a temp file
                    import tempfile
                    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.' + document_scan.file.name.split('.')[-1])
                    tmp.write(document_scan.file.read())
                    tmp.close()
                    tmp_path = tmp.name

                result = dext.extract_from_file(tmp_path)
                if result.get('success'):
                    text = result.get('raw_text', '')
                else:
                    logger.warning(f"Document extraction failed: {result.get('error')}")
                    return {'error': result.get('error', 'Extraction failed')}
            except Exception as e:
                logger.warning(f"Could not extract file using PDF extractor: {e}")
                try:
                    text = document_scan.file.read().decode('utf-8')
                except Exception:
                    logger.warning(f"Could not read file as text: {document_scan.file.name}")
                    return {'error': 'Could not extract text from document'}
        
        # Normalize text and focus on the details block under the "Proforma Invoice" heading
        try:
            if text:
                norm = text.replace('\r', '')
                idx = norm.lower().find('proforma invoice')
                if idx != -1:
                    text = norm[idx:]
                else:
                    text = norm
        except Exception:
            pass

        # If we used the DocumentExtractor earlier (result variable), merge its structured output
        merged_result = None
        try:
            if 'result' in locals() and isinstance(result, dict) and result.get('success'):
                doc_struct = result.get('structured_data', {}) or {}
                # Use InvoiceExtractor to get field-level parsing and service template matching
                invoice_fields = extractor.extract_all(text)

                # Merge doc_struct into invoice_fields, preferring invoice_fields but filling gaps
                merged = dict(invoice_fields)
                for k, v in doc_struct.items():
                    if not merged.get(k) and v:
                        merged[k] = v
                    elif isinstance(v, list) and isinstance(merged.get(k), list):
                        # combine unique
                        merged[k] = list(dict.fromkeys(merged.get(k) + v))

                # Normalize and enrich fields
                merged['raw_text'] = text[:10000]

                # Extract code no if present
                if not merged.get('code_no') and not merged.get('customer_code'):
                    m_code = re.search(r'(?mi)^\s*Code\s*No\.?\s*[:\-]?\s*([A-Z0-9\-/]+)\s*$', text)
                    if m_code:
                        merged['customer_code'] = m_code.group(1).strip()

                # Extract reference and map to plate if reference contains 'FOR T' or similar
                if not merged.get('reference'):
                    m_ref = re.search(r'Reference\s*[:\-]?\s*([A-Z0-9\s-]{3,30})', text, re.IGNORECASE)
                    if m_ref:
                        merged['reference'] = m_ref.group(1).strip()

                # Heuristic: use reference as plate only if it matches a plate-like pattern
                ref_val = merged.get('reference')
                if ref_val and not merged.get('plate_number'):
                    m_for = re.search(r'([A-Z]{1,3}\s?\d{3,5}\s?[A-Z]{0,3})', ref_val, re.IGNORECASE)
                    if m_for:
                        merged['plate_number'] = m_for.group(1).strip()

                # Ensure vehicle plate from doc_struct if available
                if not merged.get('plate_number') and doc_struct.get('vehicle_plates'):
                    merged['plate_number'] = doc_struct.get('vehicle_plates')[0]

                # Consolidate items: prefer invoice_fields items, fall back to doc_struct or DocumentExtractor
                items = []
                if isinstance(merged.get('items'), list):
                    items = merged.get('items')
                elif isinstance(doc_struct.get('items'), list):
                    items = doc_struct.get('items')
                else:
                    try:
                        # Use DocumentExtractor fallback
                        from tracker.utils.document_extraction import DocumentExtractor
                        dext_local = DocumentExtractor()
                        items = dext_local._extract_items(text)
                    except Exception:
                        items = []

                # Normalize item numeric fields
                normalized_items = []
                from decimal import Decimal, InvalidOperation
                for idx, it in enumerate(items, start=1):
                    code = (it.get('code') or it.get('item_code') or '').strip()
                    desc = (it.get('description') or it.get('desc') or '').strip()
                    qty = it.get('qty')
                    rate = it.get('rate') or it.get('rate_tsh')
                    value = it.get('value') or it.get('amount') or it.get('value_tsh')
                    net_val = it.get('net_value')
                    vat_val = it.get('vat')
                    gross_val = it.get('gross_value')

                    def _to_decimal(v):
                        try:
                            if v is None:
                                return None
                            if isinstance(v, (int, float, Decimal)):
                                return Decimal(str(v))
                            s = str(v).replace(',', '').strip()
                            return Decimal(s)
                        except (InvalidOperation, Exception):
                            return None

                    qty_d = _to_decimal(qty)
                    rate_d = _to_decimal(rate)
                    value_d = _to_decimal(value)
                    net_d = _to_decimal(net_val)
                    vat_d = _to_decimal(vat_val)
                    gross_d = _to_decimal(gross_val)

                    normalized_items.append({
                        'line_no': idx,
                        'code': code or None,
                        'description': desc or None,
                        'qty': qty_d,
                        'unit': it.get('unit') or None,
                        'rate': rate_d,
                        'value': value_d,
                        'net_value': net_d,
                        'vat': vat_d,
                        'gross_value': gross_d,
                    })

                if normalized_items:
                    merged['items'] = normalized_items

                # Prefer a service description from the first real item if header noise detected
                def _looks_like_noise(s: str) -> bool:
                    if not s:
                        return True
                    s_up = s.strip().upper()
                    return ('PROFORMA INVOICE' in s_up) or ('CODE DESCRIPTION' in s_up) or (len(s.strip()) < 3)

                if _looks_like_noise(merged.get('service_description') or ''):
                    try:
                        cand = next((it.get('description') for it in normalized_items if (it.get('description') or '').strip()), '')
                        if cand:
                            merged['service_description'] = cand
                            merged['item_name'] = cand
                    except Exception:
                        pass

                # Try to extract totals (net, VAT, gross) using regex – scoped to labeled blocks
                try:
                    m_vat = re.search(r'(?mi)^\s*VAT\s*[:\s]*([\d,]+\.?\d{0,2})\s*$', text)
                    if m_vat:
                        merged['vat_amount'] = m_vat.group(1).replace(',', '').strip()
                    m_net = re.search(r'(?mi)^\s*Net\s*Value\s*[:\s]*([\d,]+\.?\d{0,2})\s*$', text)
                    if m_net:
                        merged['net_value'] = m_net.group(1).replace(',', '').strip()
                    m_gross = re.search(r'(?mi)^\s*Gross\s*Value\s*[:\s]*([\d,]+\.?\d{0,2})\s*$', text)
                    if m_gross:
                        merged['gross_value'] = m_gross.group(1).replace(',', '').strip()
                except Exception:
                    pass

                # Attempt to match service template if not already matched
                service_desc = merged.get('service_description') or merged.get('item_name') or merged.get('description')
                if service_desc:
                    match = extractor.match_service_template(service_desc)
                    if match:
                        merged['matched_service'] = match[0]
                        merged['estimated_minutes'] = match[1]

                merged_result = merged
            else:
                # No DocumentExtractor result; fall back to InvoiceExtractor output
                extracted_data = extractor.extract_all(text)
                extracted_data['raw_text'] = text[:5000]
                merged_result = extracted_data
        except Exception as e:
            logger.warning(f"Error merging extraction results: {e}")
            extracted_data = extractor.extract_all(text)
            extracted_data['raw_text'] = text[:5000]
            merged_result = extracted_data

        # Final cleanup: plate should not be mistaken from P.O. Box/BOX lines
        try:
            plate = merged_result.get('plate_number')
            if plate:
                up = plate.upper().replace('.', ' ').strip()
                if 'BOX' in up or 'P O BOX' in up or 'P O  BOX' in up or 'P.O' in up:
                    merged_result.pop('plate_number', None)
        except Exception:
            pass

        return merged_result
    
    except Exception as e:
        logger.error(f"Error processing invoice extraction: {str(e)}")
        return {'error': str(e)}


# Global extractor instance
_extractor_instance = None

def get_extractor() -> InvoiceExtractor:
    """Get or create global extractor instance."""
    global _extractor_instance
    if _extractor_instance is None:
        _extractor_instance = InvoiceExtractor()
    return _extractor_instance
