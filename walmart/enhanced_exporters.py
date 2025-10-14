"""
Enhanced Export System
Better CSV structure, proper data types, custom field selection, and Excel support
"""
import csv
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
import pandas as pd
from config import get_config

def _timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def ensure_output_dir() -> str:
    cfg = get_config()
    os.makedirs(cfg.output_dir, exist_ok=True)
    return cfg.output_dir

# Define required column structure matching integration requirements
REQUIRED_COLUMN_ORDER = [
    "listing_title",      # 1. Listing Title
    "listing_url",        # 2. Listing URL  
    "image_url",          # 3. Image URL
    "marketplace",        # 4. Marketplace
    "price",              # 5. Price*
    "currency",           # 6. Currency
    "shipping",           # 7. Shipping
    "units_available",    # 8. Units Available
    "item_number",        # 9. Item Number
    "seller_name",        # 10. Seller's Name
    "seller_url",         # 11. Seller's URL
    "seller_business",    # 12. Seller's Business
    "seller_address",     # 13. Seller's Address
    "seller_email",       # 14. Seller's Email
    "seller_phone"        # 15. Seller's Phone
]

# Legacy column mapping for backward compatibility
LEGACY_TO_REQUIRED_MAPPING = {
    "title": "listing_title",
    "listing_url": "listing_url", 
    "url": "listing_url",
    "product_url": "listing_url",
    "image_url": "image_url",
    "product_image": "image_url",
    "marketplace": "marketplace",
    "domain": "marketplace",
    "price": "price",
    "currency": "currency",
    "shipping_info": "shipping",
    "shipping": "shipping",
    "delivery_time": "shipping",
    "units_available": "units_available",
    "availability": "units_available",
    "in_stock": "units_available",
    "item_id": "item_number",
    "item_number": "item_number",
    "sku": "item_number",
    "upc": "item_number",
    "seller_name": "seller_name",
    "seller_url": "seller_url",
    "seller_profile_url": "seller_url",
    "business_legal_name": "seller_business",
    "seller_business": "seller_business",
    "company_name": "seller_business",
    "address": "seller_address",
    "seller_address": "seller_address",
    "location": "seller_address",
    "seller_email": "seller_email",
    "email_address": "seller_email",
    "email": "seller_email",
    "phone_number": "seller_phone",
    "seller_phone": "seller_phone",
    "phone": "seller_phone",
    "contact_phone": "seller_phone"
}

# Field type definitions for proper data formatting
FIELD_TYPES = {
    "listing_title": "string",
    "listing_url": "string",
    "image_url": "string",
    "marketplace": "string",
    "price": "float",
    "currency": "string",
    "shipping": "string",
    "units_available": "int",
    "item_number": "string",
    "seller_name": "string",
    "seller_url": "string",
    "seller_business": "string",
    "seller_address": "string",
    "seller_email": "string",
    "seller_phone": "string"
}

class EnhancedCSVExporter:
    """Enhanced CSV exporter with better structure and data types"""
    
    def __init__(self, custom_fields: Optional[List[str]] = None):
        self.custom_fields = custom_fields
        self.column_order = self._determine_column_order()
    
    def _determine_column_order(self) -> List[str]:
        """Determine column order based on custom fields or required order"""
        if self.custom_fields:
            # Use custom fields but maintain logical grouping
            ordered_fields = []
            
            # Add core fields first if they exist
            core_fields = ["listing_title", "listing_url", "price", "seller_name"]
            for field in core_fields:
                if field in self.custom_fields:
                    ordered_fields.append(field)
            
            # Add remaining custom fields
            for field in self.custom_fields:
                if field not in ordered_fields:
                    ordered_fields.append(field)
            
            return ordered_fields
        
        return REQUIRED_COLUMN_ORDER
    
    def _transform_record_to_required_format(self, record: Dict[str, Any], domain: str = "Walmart") -> Dict[str, Any]:
        """Transform record to match required integration format"""
        transformed = {}
        
        # Map legacy fields to required fields
        for legacy_field, value in record.items():
            if legacy_field in LEGACY_TO_REQUIRED_MAPPING:
                required_field = LEGACY_TO_REQUIRED_MAPPING[legacy_field]
                transformed[required_field] = value
        
        # Set marketplace based on domain
        if "marketplace" not in transformed:
            marketplace_map = {
                "walmart.com": "Walmart US",
                "walmart.ca": "Walmart CA", 
                "amazon.com": "Amazon US",
                "amazon.ca": "Amazon CA",
                "ebay.com": "eBay US",
                "ebay.ca": "eBay CA",
                "shopee.sg": "Shopee SG",
                "shopee.in": "Shopee IN",
                "lazada.com.my": "Lazada MY",
                "lazada.sg": "Lazada SG"
            }
            transformed["marketplace"] = marketplace_map.get(domain.lower(), domain)
        
        # Ensure all required fields are present
        for field in REQUIRED_COLUMN_ORDER:
            if field not in transformed:
                transformed[field] = ""
        
        return transformed
    
    def _format_value(self, value: Any, field_name: str) -> Any:
        """Format value according to field type"""
        if value is None or value == "":
            return ""
        
        field_type = FIELD_TYPES.get(field_name, "string")
        
        try:
            if field_type == "float":
                if isinstance(value, str):
                    # Remove currency symbols and commas
                    cleaned = value.replace("$", "").replace(",", "").strip()
                    return float(cleaned) if cleaned else 0.0
                return float(value)
            
            elif field_type == "int":
                if isinstance(value, str):
                    # Remove commas and convert
                    cleaned = value.replace(",", "").strip()
                    return int(float(cleaned)) if cleaned else 0
                return int(value)
            
            elif field_type == "boolean":
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ["true", "yes", "1", "in stock", "available"]
                return bool(value)
            
            elif field_type == "datetime":
                if isinstance(value, str):
                    return value  # Keep as string for CSV compatibility
                return str(value)
            
            else:  # string or default
                return str(value).strip()
                
        except (ValueError, TypeError):
            # If conversion fails, return original value as string
            return str(value) if value is not None else ""
    
    def export_csv(self, records: List[Dict[str, Any]], name_prefix: str, 
                   include_metadata: bool = True, domain: str = "Walmart") -> str:
        """Export records to CSV with required integration format"""
        if not records:
            return self._create_empty_csv(name_prefix)
        
        # Transform records to required format
        transformed_records = []
        for record in records:
            transformed_record = self._transform_record_to_required_format(record, domain)
            transformed_records.append(transformed_record)
        
        # Use required column order
        final_columns = REQUIRED_COLUMN_ORDER.copy()
        
        # Create CSV file
        output_dir = ensure_output_dir()
        path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.csv")
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=final_columns, extrasaction="ignore")
            writer.writeheader()
            
            for record in transformed_records:
                # Format each field according to its type
                formatted_record = {}
                for field in final_columns:
                    formatted_record[field] = self._format_value(record.get(field), field)
                writer.writerow(formatted_record)
        
        return path
    
    def _create_empty_csv(self, name_prefix: str) -> str:
        """Create empty CSV with headers"""
        output_dir = ensure_output_dir()
        path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.csv")
        
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if self.custom_fields:
                writer.writerow(self.custom_fields)
            else:
                writer.writerow(REQUIRED_COLUMN_ORDER)
        
        return path

class ExcelExporter:
    """Excel exporter with multiple sheets and formatting"""
    
    def export_excel(self, records: List[Dict[str, Any]], offers: List[Dict[str, Any]], 
                    name_prefix: str) -> str:
        """Export to Excel with multiple sheets"""
        if not records and not offers:
            return self._create_empty_excel(name_prefix)
        
        output_dir = ensure_output_dir()
        path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.xlsx")
        
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            # Products sheet
            if records:
                df_products = pd.DataFrame(records)
                df_products.to_excel(writer, sheet_name='Products', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Products']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Offers sheet
            if offers:
                df_offers = pd.DataFrame(offers)
                df_offers.to_excel(writer, sheet_name='Offers', index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Offers']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Summary sheet
            summary_data = {
                'Metric': [
                    'Total Products',
                    'Total Offers', 
                    'Export Date',
                    'Data Quality Score',
                    'Unique Sellers'
                ],
                'Value': [
                    len(records),
                    len(offers),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    self._calculate_quality_score(records),
                    len(set(record.get('seller_name', '') for record in records if record.get('seller_name')))
                ]
            }
            df_summary = pd.DataFrame(summary_data)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
        
        return path
    
    def _calculate_quality_score(self, records: List[Dict[str, Any]]) -> str:
        """Calculate data quality score"""
        if not records:
            return "0%"
        
        total_fields = len(records) * len(STANDARD_COLUMN_ORDER[:10])  # Core fields
        filled_fields = 0
        
        for record in records:
            for field in STANDARD_COLUMN_ORDER[:10]:  # Core fields
                if record.get(field) and str(record.get(field)).strip():
                    filled_fields += 1
        
        score = (filled_fields / total_fields) * 100 if total_fields > 0 else 0
        return f"{score:.1f}%"
    
    def _create_empty_excel(self, name_prefix: str) -> str:
        """Create empty Excel file"""
        output_dir = ensure_output_dir()
        path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.xlsx")
        
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            pd.DataFrame({'message': ['No data available']}).to_excel(
                writer, sheet_name='Summary', index=False
            )
        
        return path

# Convenience functions for backward compatibility
def export_csv_enhanced(records: List[Dict[str, Any]], name_prefix: str, 
                       custom_fields: Optional[List[str]] = None,
                       include_metadata: bool = True, domain: str = "Walmart") -> str:
    """Enhanced CSV export with required integration format"""
    exporter = EnhancedCSVExporter(custom_fields)
    return exporter.export_csv(records, name_prefix, include_metadata, domain)

def export_excel(records: List[Dict[str, Any]], offers: Optional[List[Dict[str, Any]]] = None,
                name_prefix: str = "walmart_export") -> str:
    """Export to Excel with multiple sheets"""
    exporter = ExcelExporter()
    return exporter.export_excel(records, offers or [], name_prefix)

# Field selection presets - using required integration format
EXPORT_PRESETS = {
    "basic": ["listing_title", "listing_url", "price", "currency", "seller_name"],
    "detailed": ["listing_title", "listing_url", "image_url", "marketplace", "price", 
                "currency", "shipping", "seller_name", "seller_email"],
    "seller_focus": ["listing_title", "price", "seller_name", "seller_email", 
                    "seller_business", "seller_address", "seller_phone"],
    "analytics": ["listing_title", "marketplace", "price", "currency", "units_available",
                 "seller_name", "item_number"],
    "integration": REQUIRED_COLUMN_ORDER,  # Full integration format
    "full": None  # All fields
}

def get_export_preset(preset_name: str) -> Optional[List[str]]:
    """Get predefined field selection"""
    return EXPORT_PRESETS.get(preset_name)

def export_json_enhanced(records: List[Dict[str, Any]], name_prefix: str, 
                        domain: str = "Walmart") -> str:
    """Export records to JSON with required integration format"""
    if not records:
        output_dir = ensure_output_dir()
        path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"message": "No records found"}, f, indent=2)
        return path
    
    # Transform records to required format
    transformed_records = []
    for record in records:
        # Create a temporary exporter to use the transformation function
        exporter = EnhancedCSVExporter()
        transformed_record = exporter._transform_record_to_required_format(record, domain)
        transformed_records.append(transformed_record)
    
    # Create JSON file
    output_dir = ensure_output_dir()
    path = os.path.join(output_dir, f"{name_prefix}_{_timestamp()}.json")
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(transformed_records, f, indent=2, ensure_ascii=False)
    
    return path

