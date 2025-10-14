"""
Data Quality Management System
Handles duplicate detection, validation, cleanup, and error handling
"""
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class DataQualityReport:
    """Report on data quality metrics"""
    total_records: int
    duplicate_count: int
    invalid_records: int
    missing_data_count: int
    quality_score: float
    issues: List[str]

@dataclass
class ValidationRule:
    """Data validation rule"""
    field: str
    required: bool
    data_type: type
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    custom_validator: Optional[callable] = None

class DuplicateDetector:
    """Advanced duplicate detection system"""
    
    def __init__(self):
        self.seen_hashes: Set[str] = set()
        self.seen_listing_ids: Set[str] = set()
        self.similarity_threshold = 0.85
        
    def generate_fingerprint(self, product_data: Dict[str, Any]) -> str:
        """Generate a unique fingerprint for duplicate detection"""
        # Extract key identifying fields
        key_fields = [
            product_data.get('item_id', ''),
            product_data.get('title', ''),
            product_data.get('brand', ''),
            product_data.get('upc', ''),
            product_data.get('model', ''),
        ]
        
        # Clean and normalize
        normalized = []
        for field in key_fields:
            if field:
                # Remove extra whitespace, convert to lowercase
                cleaned = re.sub(r'\s+', ' ', str(field).strip().lower())
                normalized.append(cleaned)
        
        # Create hash
        fingerprint = hashlib.md5('|'.join(normalized).encode()).hexdigest()
        return fingerprint
    
    def is_duplicate(self, product_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Check if product is a duplicate"""
        fingerprint = self.generate_fingerprint(product_data)
        
        # Check exact fingerprint match
        if fingerprint in self.seen_hashes:
            return True, f"Exact duplicate (fingerprint: {fingerprint[:8]}...)"
        
        # Check listing ID match
        listing_id = product_data.get('item_id', '')
        if listing_id and listing_id in self.seen_listing_ids:
            return True, f"Duplicate listing ID: {listing_id}"
        
        # Check similarity for potential duplicates
        title = product_data.get('title', '')
        if title:
            similarity_score = self._calculate_title_similarity(title)
            if similarity_score > self.similarity_threshold:
                return True, f"Similar product (similarity: {similarity_score:.2f})"
        
        # Mark as seen
        self.seen_hashes.add(fingerprint)
        if listing_id:
            self.seen_listing_ids.add(listing_id)
        
        return False, ""
    
    def _calculate_title_similarity(self, title: str) -> float:
        """Calculate similarity score with existing titles"""
        if not title:
            return 0.0
        
        # Simple similarity based on common words
        title_words = set(re.findall(r'\w+', title.lower()))
        max_similarity = 0.0
        
        # This is a simplified version - in production, you'd use more sophisticated
        # similarity algorithms like Levenshtein distance or TF-IDF
        for seen_title in [""]:  # Placeholder for existing titles
            if not seen_title:
                continue
            seen_words = set(re.findall(r'\w+', seen_title.lower()))
            
            if title_words and seen_words:
                intersection = len(title_words & seen_words)
                union = len(title_words | seen_words)
                similarity = intersection / union if union > 0 else 0.0
                max_similarity = max(max_similarity, similarity)
        
        return max_similarity

class DataValidator:
    """Comprehensive data validation system"""
    
    def __init__(self):
        self.validation_rules = self._setup_validation_rules()
        self.quality_threshold = 0.95  # 95% data integrity target
    
    def _setup_validation_rules(self) -> List[ValidationRule]:
        """Setup validation rules for Walmart product data"""
        return [
            ValidationRule('item_id', True, str, min_length=1),
            ValidationRule('title', True, str, min_length=3, max_length=500),
            ValidationRule('price', False, (int, float, str)),
            ValidationRule('brand', False, str, max_length=100),
            ValidationRule('category', False, str),
            ValidationRule('availability', False, str),
            ValidationRule('seller_name', False, str, max_length=200),
            ValidationRule('rating', False, (int, float)),
            ValidationRule('review_count', False, int),
            ValidationRule('url', False, str, pattern=r'^https?://'),
        ]
    
    def validate_record(self, product_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validate a single product record"""
        issues = []
        
        for rule in self.validation_rules:
            field_value = product_data.get(rule.field)
            
            # Check required fields
            if rule.required and (field_value is None or field_value == ''):
                issues.append(f"Missing required field: {rule.field}")
                continue
            
            if field_value is not None and field_value != '':
                # Check data type
                if not isinstance(field_value, rule.data_type):
                    if not (isinstance(field_value, str) and rule.data_type in (int, float)):
                        issues.append(f"Invalid type for {rule.field}: expected {rule.data_type.__name__}")
                
                # Check length constraints
                if isinstance(field_value, str):
                    if rule.min_length and len(field_value) < rule.min_length:
                        issues.append(f"{rule.field} too short (min: {rule.min_length})")
                    if rule.max_length and len(field_value) > rule.max_length:
                        issues.append(f"{rule.field} too long (max: {rule.max_length})")
                    
                    # Check pattern
                    if rule.pattern and not re.match(rule.pattern, field_value):
                        issues.append(f"{rule.field} format invalid")
                
                # Custom validation
                if rule.custom_validator:
                    try:
                        if not rule.custom_validator(field_value):
                            issues.append(f"{rule.field} failed custom validation")
                    except Exception as e:
                        issues.append(f"{rule.field} validation error: {str(e)}")
        
        return len(issues) == 0, issues
    
    def calculate_quality_score(self, records: List[Dict[str, Any]]) -> DataQualityReport:
        """Calculate overall data quality score"""
        total_records = len(records)
        invalid_records = 0
        missing_data_count = 0
        all_issues = []
        
        for record in records:
            is_valid, issues = self.validate_record(record)
            if not is_valid:
                invalid_records += 1
                all_issues.extend(issues)
            
            # Count missing critical data
            critical_fields = ['title', 'price', 'availability']
            missing_critical = sum(1 for field in critical_fields if not record.get(field))
            missing_data_count += missing_critical
        
        duplicate_detector = DuplicateDetector()
        duplicate_count = 0
        for record in records:
            is_dup, _ = duplicate_detector.is_duplicate(record)
            if is_dup:
                duplicate_count += 1
        
        # Calculate quality score
        quality_score = 1.0 - (invalid_records + duplicate_count + missing_data_count * 0.1) / total_records
        quality_score = max(0.0, min(1.0, quality_score))
        
        return DataQualityReport(
            total_records=total_records,
            duplicate_count=duplicate_count,
            invalid_records=invalid_records,
            missing_data_count=missing_data_count,
            quality_score=quality_score,
            issues=all_issues[:20]  # Limit issues list
        )

class DataCleaner:
    """Automated data cleanup and normalization"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters that might cause issues
        cleaned = re.sub(r'[^\w\s\-.,!?()&]', '', cleaned)
        
        return cleaned
    
    @staticmethod
    def normalize_price(price: Any) -> Optional[float]:
        """Normalize price data"""
        if price is None:
            return None
        
        if isinstance(price, (int, float)):
            return float(price)
        
        if isinstance(price, str):
            # Extract numeric value from price string
            price_match = re.search(r'[\d,]+\.?\d*', price.replace(',', ''))
            if price_match:
                try:
                    return float(price_match.group())
                except ValueError:
                    pass
        
        return None
    
    @staticmethod
    def normalize_rating(rating: Any) -> Optional[float]:
        """Normalize rating data"""
        if rating is None:
            return None
        
        if isinstance(rating, (int, float)):
            return float(rating)
        
        if isinstance(rating, str):
            try:
                return float(rating)
            except ValueError:
                pass
        
        return None
    
    @staticmethod
    def clean_product_record(product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a complete product record"""
        cleaned = product_data.copy()
        
        # Clean text fields
        text_fields = ['title', 'brand', 'category', 'seller_name', 'description']
        for field in text_fields:
            if field in cleaned and cleaned[field]:
                cleaned[field] = DataCleaner.clean_text(str(cleaned[field]))
        
        # Normalize numeric fields
        if 'price' in cleaned:
            cleaned['price'] = DataCleaner.normalize_price(cleaned['price'])
        
        if 'rating' in cleaned:
            cleaned['rating'] = DataCleaner.normalize_rating(cleaned['rating'])
        
        # Ensure item_id is string
        if 'item_id' in cleaned:
            cleaned['item_id'] = str(cleaned['item_id'])
        
        # Clean URL
        if 'url' in cleaned and cleaned['url']:
            url = cleaned['url'].strip()
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            cleaned['url'] = url
        
        return cleaned

class ErrorHandler:
    """Graceful error handling for missing/incomplete data"""
    
    @staticmethod
    def handle_missing_data(product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle missing data gracefully"""
        handled = product_data.copy()
        
        # Set default values for missing critical fields
        defaults = {
            'title': 'Unknown Product',
            'price': 0.0,
            'availability': 'Unknown',
            'rating': 0.0,
            'review_count': 0,
            'seller_name': 'Unknown Seller',
            'category': 'Uncategorized'
        }
        
        for field, default_value in defaults.items():
            if field not in handled or handled[field] is None or handled[field] == '':
                handled[field] = default_value
                logger.warning(f"Missing {field} for product {handled.get('item_id', 'unknown')}, using default")
        
        return handled
    
    @staticmethod
    def validate_and_fix_record(product_data: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Validate and fix a record, returning cleaned data and any warnings"""
        warnings = []
        
        # Handle missing data
        fixed_data = ErrorHandler.handle_missing_data(product_data)
        
        # Clean the data
        cleaned_data = DataCleaner.clean_product_record(fixed_data)
        
        # Validate the cleaned data
        validator = DataValidator()
        is_valid, issues = validator.validate_record(cleaned_data)
        
        if not is_valid:
            warnings.extend(issues)
            logger.warning(f"Data quality issues for product {cleaned_data.get('item_id', 'unknown')}: {issues}")
        
        return cleaned_data, warnings

class DataQualityManager:
    """Main data quality management system"""
    
    def __init__(self):
        self.duplicate_detector = DuplicateDetector()
        self.validator = DataValidator()
        self.cleaner = DataCleaner()
        self.error_handler = ErrorHandler()
    
    def process_batch(self, records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], DataQualityReport]:
        """Process a batch of records with full quality management"""
        processed_records = []
        all_warnings = []
        
        for record in records:
            # Check for duplicates
            is_duplicate, dup_reason = self.duplicate_detector.is_duplicate(record)
            if is_duplicate:
                logger.info(f"Skipping duplicate: {dup_reason}")
                continue
            
            # Validate and fix record
            cleaned_record, warnings = self.error_handler.validate_and_fix_record(record)
            processed_records.append(cleaned_record)
            all_warnings.extend(warnings)
        
        # Generate quality report
        quality_report = self.validator.calculate_quality_score(processed_records)
        quality_report.issues.extend(all_warnings[:10])  # Add warnings to issues
        
        logger.info(f"Data quality processing complete: {quality_report.quality_score:.2%} quality score")
        
        return processed_records, quality_report
    
    def get_quality_summary(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get a quick quality summary for monitoring"""
        report = self.validator.calculate_quality_score(records)
        
        return {
            'total_records': report.total_records,
            'quality_score': report.quality_score,
            'quality_percentage': f"{report.quality_score:.1%}",
            'meets_threshold': report.quality_score >= 0.95,
            'duplicates_removed': report.duplicate_count,
            'invalid_records': report.invalid_records,
            'missing_data_issues': report.missing_data_count,
            'status': '✅ Excellent' if report.quality_score >= 0.95 else '⚠️ Needs Attention'
        }

