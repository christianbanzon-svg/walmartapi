import argparse
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from bluecart_client import BlueCartClient
from config import get_config
from storage import init_db, insert_listing_snapshot, insert_seller_snapshot, upsert_listing_summary
from exporters import export_json, export_csv, write_debug_json
# Import enhanced exporters for integration format
try:
    from enhanced_exporters import export_csv_enhanced, export_json_enhanced
    ENHANCED_EXPORTS_AVAILABLE = True
except ImportError:
    ENHANCED_EXPORTS_AVAILABLE = False
# Removed imports for deleted modules - using simplified version


def _safe_get(d: Dict[str, Any], *keys, default=None):
	cur = d
	for k in keys:
		if not isinstance(cur, dict) or k not in cur:
			return default
		cur = cur[k]
	return cur


def _ts() -> str:
	return datetime.utcnow().strftime("%H:%M:%S")


def _extract_seller_fields(sp: Dict[str, Any]) -> Dict[str, Any]:
	"""Normalize BlueCart seller_profile response to our unified seller fields.

	Handles responses where details live under 'seller_details' or at top-level.
	"""
	if not isinstance(sp, dict):
		return {}
	# Prefer nested seller_details when present
	node: Dict[str, Any] = sp.get("seller_details") if isinstance(sp.get("seller_details"), dict) else sp
	# Address normalization
	address_text: Optional[str] = node.get("address_text") or node.get("address") if isinstance(node.get("address"), str) else None
	addr_obj = node.get("address") if isinstance(node.get("address"), dict) else None
	country = None
	state_province = None
	zip_code = None
	if addr_obj:
		parts = [
			addr_obj.get("address1") or addr_obj.get("street1") or addr_obj.get("streetAddress"),
			addr_obj.get("city") or addr_obj.get("addressLocality"),
			addr_obj.get("state") or addr_obj.get("addressRegion"),
			addr_obj.get("category_id") or addr_obj.get("postalCode") or addr_obj.get("zip"),
			addr_obj.get("country") or addr_obj.get("addressCountry"),
		]
		address_text = address_text or " ".join([p for p in parts if p])
		country = (addr_obj.get("country") or addr_obj.get("addressCountry")) if not isinstance(addr_obj.get("addressCountry"), dict) else None
		state_province = addr_obj.get("state") or addr_obj.get("addressRegion")
		zip_code = addr_obj.get("category_id") or addr_obj.get("postalCode") or addr_obj.get("zip")
	# Reviews total
	rating_breakdown = node.get("rating_breakdown") if isinstance(node.get("rating_breakdown"), dict) else None
	total_reviews_calc = sum(int(v) for v in rating_breakdown.values()) if rating_breakdown else None
	return {
		"seller_profile_picture": node.get("logo") or node.get("image"),
		"seller_profile_url": node.get("seller_url") or node.get("url") or node.get("link"),
		"business_legal_name": node.get("name") or node.get("legal_name"),
		"email_address": node.get("email"),
		"phone_number": node.get("phone") or node.get("telephone"),
		"address": address_text,
		"country": country,
		"state_province": state_province,
		"zip_code": zip_code,
		"seller_rating": node.get("rating"),
		"total_reviews": node.get("reviews_count") or total_reviews_calc,
	}


def _is_numeric_string(value: Optional[str]) -> bool:
	try:
		return value is not None and str(int(str(value))) == str(value)
	except Exception:
		return False


def _collect_numeric_seller_id(node: Any) -> Optional[str]:
	"""Walk arbitrary dict/list to find a numeric seller id if present."""
	try:
		if isinstance(node, dict):
			# direct fields
			for key in ("seller_id", "sellerId", "id"):
				val = node.get(key)
				if isinstance(val, (str, int)) and _is_numeric_string(str(val)):
					return str(val)
			# nested seller object
			sel = node.get("seller")
			if isinstance(sel, dict):
				val = sel.get("id")
				if isinstance(val, (str, int)) and _is_numeric_string(str(val)):
					return str(val)
			# recurse
			for v in node.values():
				found = _collect_numeric_seller_id(v)
				if found:
					return found
		elif isinstance(node, list):
			for it in node:
				found = _collect_numeric_seller_id(it)
				if found:
					return found
		return None
	except Exception:
		return None


def is_brand_match(keyword: str, product_title: str, product_brand: str, raw_product: Optional[Dict[str, Any]] = None) -> bool:
	"""
	Check if product matches the brand keyword.
	Filters out false positives like "Colgate Triple Action" when searching for "Triple Paste".
	
	Args:
		keyword: The search keyword (e.g., "Triple Paste")
		product_title: Product title
		product_brand: Product brand name
		raw_product: Raw product data for additional checks
	
	Returns:
		True if product matches brand, False if it's a false positive
	"""
	if not keyword or not product_title:
		return True  # If no keyword or title, don't filter (let it through)
	
	keyword_lower = keyword.lower().strip()
	title_lower = (product_title or "").lower()
	brand_lower = (product_brand or "").lower()
	
	# Extract brand name from keyword (e.g., "Triple Paste" -> ["triple", "paste"])
	# For single-word brands, use the whole word
	keyword_words = keyword_lower.split()
	if len(keyword_words) == 1:
		# Single word brand - check if it appears as a brand name
		brand_match = keyword_lower in brand_lower or keyword_lower in title_lower
		if not brand_match:
			return False
	else:
		# Multi-word brand - check if all significant words appear
		# Filter out common words like "the", "and", "of"
		significant_words = [w for w in keyword_words if len(w) > 2 and w not in ["the", "and", "of", "for"]]
		if not significant_words:
			significant_words = keyword_words
		
		# Check if brand appears in title or brand field
		brand_in_title = all(word in title_lower for word in significant_words)
		brand_in_brand_field = all(word in brand_lower for word in significant_words) if brand_lower else False
		
		if not brand_in_title and not brand_in_brand_field:
			return False
	
	# Check for common false positives
	false_positive_map = {
		"triple paste": {
			"exclude_if_contains": ["colgate", "crest", "mutti", "triple action", "toothpaste", "dental"],
			"require_contains": ["paste", "diaper", "rash", "ointment", "cream"]
		},
		"amlactin": {
			"exclude_if_contains": ["lactic", "acid", "moisturizer"],
			"require_contains": ["amlactin"]
		},
		"kerasal": {
			"exclude_if_contains": [],
			"require_contains": ["kerasal"]
		},
		"dermoplast": {
			"exclude_if_contains": [],
			"require_contains": ["dermoplast"]
		},
		"new-skin": {
			"exclude_if_contains": [],
			"require_contains": ["new-skin", "new skin"]
		},
		"domeboro": {
			"exclude_if_contains": [],
			"require_contains": ["domeboro"]
		}
	}
	
	# Check false positive rules
	for brand_key, rules in false_positive_map.items():
		if brand_key in keyword_lower:
			# Check exclusion rules
			exclude_keywords = rules.get("exclude_if_contains", [])
			for exclude_kw in exclude_keywords:
				if exclude_kw in title_lower and not any(req in title_lower for req in rules.get("require_contains", [])):
					return False
			
			# Check requirement rules
			require_keywords = rules.get("require_contains", [])
			if require_keywords:
				if not any(req in title_lower or req in brand_lower for req in require_keywords):
					return False
	
	return True


def validate_price_and_stock(price: Optional[float], units_available: Optional[int] = None) -> Tuple[bool, Dict[str, Any]]:
	"""
	Validate price and determine stock status.
	
	Args:
		price: Product price
		units_available: Available units/quantity
	
	Returns:
		Tuple of (is_valid, stock_info_dict)
		stock_info_dict contains: price, stock_status, units_available
	"""
	if price is None or price == 0.00:
		return False, {
			"price": "",
			"stock_status": "Out of Stock",
			"units_available": ""
		}
	
	if not isinstance(price, (int, float)) or price < 0:
		return False, {
			"price": "",
			"stock_status": "Invalid Price",
			"units_available": ""
		}
	
	# Determine stock status
	if units_available is None or units_available == 0:
		stock_status = "Out of Stock"
		units_available_str = ""
	else:
		stock_status = "In Stock"
		units_available_str = str(units_available)
	
	return True, {
		"price": price,
		"stock_status": stock_status,
		"units_available": units_available_str
	}


def collect_upc_from_multiple_sources(raw_product: Dict[str, Any], product_resp: Optional[Dict[str, Any]] = None, raw_search: Optional[Dict[str, Any]] = None) -> Optional[str]:
	"""
	Collect UPC from multiple sources to maximize coverage.
	
	Checks in order:
	1. Product API response (product.upc, product.gtin)
	2. Search results (product.upc, product.gtin)
	3. Product variants (if available)
	4. Additional product fields
	
	Returns:
		UPC string or None if not found
	"""
	upc = None
	
	# Try Product API response first (most reliable)
	if product_resp:
		product_data = _safe_get(product_resp, "product", default={}) or product_resp
		upc = (
			product_data.get("upc") or
			product_data.get("gtin") or
			product_data.get("gtin14") or
			product_data.get("ean") or
			_safe_get(product_data, "identifiers", "upc") or
			_safe_get(product_data, "identifiers", "gtin")
		)
		if upc:
			return str(upc).strip()
	
	# Try raw product from search results
	if raw_product:
		upc = (
			raw_product.get("upc") or
			raw_product.get("gtin") or
			raw_product.get("gtin14") or
			raw_product.get("ean") or
			_safe_get(raw_product, "identifiers", "upc") or
			_safe_get(raw_product, "identifiers", "gtin")
		)
		if upc:
			return str(upc).strip()
	
	# Try raw search item
	if raw_search:
		product_from_search = _safe_get(raw_search, "product", default={})
		if product_from_search:
			upc = (
				product_from_search.get("upc") or
				product_from_search.get("gtin") or
				product_from_search.get("gtin14") or
				product_from_search.get("ean")
			)
			if upc:
				return str(upc).strip()
		
		# Check variants
		variants = _safe_get(raw_search, "product", "variants") or _safe_get(raw_search, "variants") or []
		if variants and isinstance(variants, list):
			for variant in variants:
				if isinstance(variant, dict):
					variant_upc = variant.get("upc") or variant.get("gtin")
					if variant_upc:
						return str(variant_upc).strip()
	
	return None


def validate_seller_url(seller_url: Optional[str], seller_id: Optional[str], seller_name: Optional[str]) -> Tuple[Optional[str], bool]:
	"""
	Validate seller URL and fix mapping errors.
	
	Args:
		seller_url: Current seller URL
		seller_id: Seller ID (numeric preferred)
		seller_name: Seller name for validation
	
	Returns:
		Tuple of (validated_url, is_valid)
	"""
	if not seller_url:
		# Construct URL from seller ID if available
		if seller_id and _is_numeric_string(str(seller_id)):
			return f"https://www.walmart.com/seller/{seller_id}", True
		return None, False
	
	# Validate URL format
	if not isinstance(seller_url, str):
		return None, False
	
	seller_url = seller_url.strip()
	
	# Check if URL is a valid Walmart seller URL
	if not seller_url.startswith("http"):
		# Might be a relative URL
		if seller_url.startswith("/seller/"):
			seller_url = f"https://www.walmart.com{seller_url}"
		elif seller_id and _is_numeric_string(str(seller_id)):
			# Construct from seller ID
			seller_url = f"https://www.walmart.com/seller/{seller_id}"
		else:
			return None, False
	
	# Extract seller ID from URL to validate
	if "/seller/" in seller_url:
		try:
			url_parts = seller_url.split("/seller/")
			if len(url_parts) > 1:
				url_seller_id = url_parts[1].split("/")[0].split("?")[0].strip()
				# If we have a seller_id, validate it matches URL
				if seller_id and _is_numeric_string(str(seller_id)):
					if str(seller_id) != url_seller_id:
						# Mismatch - reconstruct URL with correct ID
						return f"https://www.walmart.com/seller/{seller_id}", True
		except Exception:
			pass
	
	return seller_url, True


def normalize_listing_from_search(item: Dict[str, Any]) -> Dict[str, Any]:
	product = _safe_get(item, "product", default={}) or {}
	offers = _safe_get(item, "offers", default={}) or {}
	primary = _safe_get(offers, "primary", default={}) or {}
	images = _safe_get(product, "images", default=[]) or []
	# Coerce images to strings if BlueCart returns objects
	coerced_images: List[str] = []
	for img in images:
		if isinstance(img, str):
			coerced_images.append(img)
		elif isinstance(img, dict):
			for key in ("url", "link", "src", "image"):
				val = img.get(key)
				if isinstance(val, str):
					coerced_images.append(val)
					break
	main_image = _safe_get(product, "main_image") or (coerced_images[0] if coerced_images else None)
	currency_symbol = _safe_get(primary, "currency_symbol")
	currency_code = {
		"$": "USD",
		"£": "GBP",
		"€": "EUR",
		"C$": "CAD",
	}.get(currency_symbol or "", None)
	return {
		"listing_id": _safe_get(product, "item_id") or _safe_get(product, "product_id"),
		"title": _safe_get(product, "title"),
		"brand": _safe_get(product, "brand"),
		"price": _safe_get(primary, "price"),
		"currency": currency_code,
		"url": _safe_get(product, "link"),
		"image": main_image,
		"asin": _safe_get(product, "asin"),
		"upc": _safe_get(product, "upc") or _safe_get(product, "gtin"),
	}



def normalize_product(item: Dict[str, Any]) -> Dict[str, Any]:
	"""Enhanced product normalization with additional fields for Phase 2."""
	images = _safe_get(item, "images") or _safe_get(item, "product", "images") or []
	# Coerce to list[str]
	coerced_images: List[str] = []
	for img in images:
		if isinstance(img, str):
			coerced_images.append(img)
		elif isinstance(img, dict):
			for key in ("url", "link", "src", "image"):
				val = img.get(key)
				if isinstance(val, str):
					coerced_images.append(val)
					break
	main_image = _safe_get(item, "main_image") or _safe_get(item, "product", "main_image")
	if not coerced_images and isinstance(main_image, str):
		coerced_images = [main_image]
	
	# Extract product data (handle nested product structure)
	product_data = item.get("product") if isinstance(item.get("product"), dict) else item
	
	# Extract category path
	category_path = None
	category_list = _safe_get(product_data, "categories") or _safe_get(product_data, "category") or []
	if isinstance(category_list, list) and len(category_list) > 0:
		# If categories is a list of strings, join them
		if all(isinstance(c, str) for c in category_list):
			category_path = " > ".join(category_list)
		# If categories is a list of objects with name/title fields
		elif all(isinstance(c, dict) for c in category_list):
			category_names = [c.get("name") or c.get("title") or "" for c in category_list if c.get("name") or c.get("title")]
			if category_names:
				category_path = " > ".join(category_names)
	elif isinstance(category_list, str):
		category_path = category_list
	elif isinstance(category_list, dict):
		category_path = category_list.get("name") or category_list.get("path") or ""
	
	# Extract dimensions
	dimensions = None
	dimensions_obj = _safe_get(product_data, "dimensions") or _safe_get(product_data, "package_dimensions") or {}
	if isinstance(dimensions_obj, dict):
		length = dimensions_obj.get("length") or dimensions_obj.get("l")
		width = dimensions_obj.get("width") or dimensions_obj.get("w")
		height = dimensions_obj.get("height") or dimensions_obj.get("h")
		unit = dimensions_obj.get("unit") or "in"
		if length and width and height:
			dimensions = f"{length} x {width} x {height} {unit}"
	
	# Extract weight
	weight = None
	weight_obj = _safe_get(product_data, "weight") or _safe_get(product_data, "package_weight") or {}
	if isinstance(weight_obj, dict):
		weight_value = weight_obj.get("value") or weight_obj.get("weight")
		weight_unit = weight_obj.get("unit") or "lbs"
		if weight_value:
			weight = f"{weight_value} {weight_unit}"
	elif isinstance(weight_obj, (int, float)):
		weight = f"{weight_obj} lbs"
	
	# Extract reviews and rating
	reviews_count = (
		_safe_get(product_data, "reviews_count") or
		_safe_get(product_data, "ratings_total") or
		_safe_get(product_data, "total_reviews") or
		_safe_get(product_data, "review_count") or
		0
	)
	product_rating = (
		_safe_get(product_data, "rating") or
		_safe_get(product_data, "average_rating") or
		_safe_get(product_data, "star_rating") or
		None
	)
	
	# Extract shipping information
	shipping_cost = None
	shipping_info = _safe_get(product_data, "shipping") or _safe_get(product_data, "shipping_info") or {}
	if isinstance(shipping_info, dict):
		shipping_cost = shipping_info.get("cost") or shipping_info.get("price") or shipping_info.get("shipping_cost")
		if shipping_cost is None:
			# Try to extract from string
			shipping_text = shipping_info.get("text") or shipping_info.get("description") or ""
			if "free" in shipping_text.lower():
				shipping_cost = "Free"
	elif isinstance(shipping_info, str):
		shipping_cost = shipping_info
	
	# Extract estimated delivery
	estimated_delivery = None
	delivery_info = _safe_get(product_data, "delivery") or _safe_get(product_data, "estimated_delivery") or _safe_get(product_data, "shipping", "estimated_delivery") or {}
	if isinstance(delivery_info, dict):
		estimated_delivery = delivery_info.get("text") or delivery_info.get("description") or delivery_info.get("days")
	elif isinstance(delivery_info, str):
		estimated_delivery = delivery_info
	
	# Extract variants
	variants = _safe_get(product_data, "variants") or _safe_get(product_data, "product_variants") or []
	variant_list = []
	if isinstance(variants, list):
		for variant in variants:
			if isinstance(variant, dict):
				variant_info = {
					"variant_id": variant.get("id") or variant.get("variant_id"),
					"title": variant.get("title") or variant.get("name"),
					"price": variant.get("price"),
					"sku": variant.get("sku"),
					"upc": variant.get("upc") or variant.get("gtin"),
					"in_stock": variant.get("in_stock"),
				}
				variant_list.append(variant_info)
	
	return {
		"listing_id": _safe_get(item, "item_id") or _safe_get(item, "product_id") or _safe_get(item, "product", "item_id") or _safe_get(item, "product", "product_id"),
		"sku": _safe_get(item, "sku") or _safe_get(item, "product", "sku"),
		"title": _safe_get(item, "title") or _safe_get(item, "product", "title"),
		"brand": _safe_get(item, "brand") or _safe_get(item, "product", "brand"),
		"description": _safe_get(item, "description") or _safe_get(item, "product", "description") or _safe_get(item, "product", "description_full"),
		"images": coerced_images,
		"asin": _safe_get(item, "asin") or _safe_get(item, "product", "asin"),
		"upc": _safe_get(item, "upc") or _safe_get(item, "product", "upc") or _safe_get(item, "product", "gtin"),
		# Phase 2: Additional fields
		"category": category_path,
		"dimensions": dimensions,
		"weight": weight,
		"product_reviews_count": reviews_count,
		"product_rating": product_rating,
		"shipping_cost": shipping_cost,
		"estimated_delivery": estimated_delivery,
		"variants": variant_list if variant_list else None,
	}


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
	return {
		"seller_id": _safe_get(offer, "seller_id") or _safe_get(offer, "seller", "id"),
		"seller_name": _safe_get(offer, "seller_name") or _safe_get(offer, "seller", "name"),
		"seller_rating": _safe_get(offer, "seller_rating") or _safe_get(offer, "seller", "rating"),
		"total_reviews": _safe_get(offer, "total_reviews") or _safe_get(offer, "seller", "reviews_count"),
		"price": _safe_get(offer, "price"),
		"currency": _safe_get(offer, "currency") or _safe_get(offer, "currency_symbol"),
		"url": _safe_get(offer, "seller_url") or _safe_get(offer, "seller", "url"),
		"quantity": _safe_get(offer, "quantity") or _safe_get(offer, "available_quantity") or _safe_get(offer, "inventory", "quantity"),
	}


def run(keyword_list: List[str], max_per_keyword: int, export: List[str], sleep: float, offers_export: bool, max_pages: int, debug: bool, walmart_domain: Optional[str] = None, category_id: Optional[str] = None, retry_seller_passes: int = 0, retry_seller_delay: float = 15.0) -> None:
	"""Main scraping function with export"""
	try:
		init_db()
		cfg = get_config()
		# Use custom domain if provided, otherwise use default from config
		domain_to_use = walmart_domain or cfg.site
		client = BlueCartClient(sleep_seconds=sleep, site=domain_to_use)
		print(f"[{_ts()}] Start scan | domain={client.site} | keywords={len(keyword_list)} | max_per_keyword={max_per_keyword} | max_pages={max_pages} | export={export}", flush=True)

		all_records: List[Dict[str, Any]] = []
		all_offers: List[Dict[str, Any]] = []
		# Cache and pending lists for seller retries
		seller_cache: Dict[str, Dict[str, Any]] = {}  # Cache seller profiles to avoid duplicate API calls
		pending_sellers: List[Tuple[Optional[str], Optional[str]]] = []

		for kw in keyword_list:
			print(f"[{_ts()}] Keyword: {kw}", flush=True)
			collected = 0
			print(f"[{_ts()}] Starting collection for keyword: {kw}", flush=True)
			page = 1
			# If max_per_keyword is 0 or negative, collect unlimited items
			# If max_pages is 0 or negative, collect unlimited pages
			max_items = max_per_keyword if max_per_keyword > 0 else float('inf')
			max_page_limit = max_pages if max_pages > 0 else float('inf')
			
			# Check if we should use Walmart's "total_results" as the limit (exact matches only)
			walmart_exact_match_limit = None
			if page == 1:
				# Get first page to check Walmart's total_results count
				extra: Dict[str, Any] = {}
				if category_id:
					extra["category_id"] = category_id
				first_page_resp = client.search(kw, page=1, extra=extra)
				pagination = first_page_resp.get("pagination", {})
				total_results = pagination.get("total_results")
				if total_results and max_per_keyword == 0:
					# If unlimited was requested, use Walmart's exact match count
					walmart_exact_match_limit = total_results
					print(f"[{_ts()}] Walmart reports {total_results} 'exact matches' (relevant results) - collecting only these", flush=True)
			
			# Use Walmart's limit if available, otherwise use max_per_keyword
			effective_max_items = walmart_exact_match_limit if walmart_exact_match_limit else max_items
			
			while collected < effective_max_items and page <= max_page_limit:
				max_display = effective_max_items if effective_max_items != float('inf') else 'unlimited'
				print(f"[{_ts()}]  Page {page} | Collected so far: {collected}/{max_display}", flush=True)
				extra: Dict[str, Any] = {}
				if category_id:
					extra["category_id"] = category_id
				search_resp = client.search(kw, page=page, extra=extra)
				if debug and page == 1:
					write_debug_json(search_resp, f"debug_search_{kw.replace(' ', '_')}.json")
				items = search_resp.get("search_results") or search_resp.get("items") or []
				# BlueCart sometimes nests results under "results" or "search_results"; also check "data" container
				if not items:
					items = (search_resp.get("results") or [])
				if not items and isinstance(search_resp.get("data"), dict):
					data = search_resp["data"]
					items = data.get("search_results") or data.get("items") or data.get("results") or []
				if not items:
					print(f"[{_ts()}]   No items returned; stopping pagination for keyword", flush=True)
					break
				print(f"[{_ts()}]   Found {len(items)} items on page {page}", flush=True)
				
				# Track items added this page to detect if we're getting duplicates
				items_added_this_page = 0
				seen_this_keyword: set = set()  # Track listing_ids seen for this keyword
				for raw in items:
					# Skip limit check if max_per_keyword is 0 or negative (unlimited)
					if max_per_keyword > 0 and collected >= max_per_keyword:
						break
					listing = normalize_listing_from_search(raw)
					listing_id = str(listing.get("listing_id")) if listing.get("listing_id") is not None else None
					if not listing_id:
						continue
					# Skip if we've already seen this listing_id in this keyword scan
					if listing_id in seen_this_keyword:
						continue
					
					# Filter out toy/diecast products for automotive searches
					# Check title and brand for toy-related keywords
					title_lower = (listing.get("title") or "").lower()
					brand_lower = (listing.get("brand") or "").lower()
					product_title_lower = (_safe_get(raw, "product", "title") or "").lower()
					
					# Keywords that indicate toys/diecast (not real automotive parts)
					toy_keywords = ["toy", "diecast", "model car", "disney", "1:64", "1:24", "1:43", "scale", "collectible", "action figure"]
					
					# Check if product is a toy
					is_toy = any(keyword in title_lower or keyword in brand_lower or keyword in product_title_lower 
								for keyword in toy_keywords)
					
					if is_toy:
						# Skip toy products for automotive searches
						continue
					
					# DATA QUALITY: Brand filtering to remove false positives
					product_title = listing.get("title") or _safe_get(raw, "product", "title") or ""
					product_brand = listing.get("brand") or _safe_get(raw, "product", "brand") or ""
					if not is_brand_match(kw, product_title, product_brand, raw_product):
						if debug:
							print(f"[{_ts()}]   ❌ Brand filter: Skipping false positive - '{product_title[:50]}...' (keyword: {kw})", flush=True)
						continue
					
					seen_this_keyword.add(listing_id)
					upsert_listing_summary(listing_id, listing.get("listing_title"), listing.get("brand"), listing.get("url"))
					# OPTIMIZATION: Try to use product data from search results first, only call API if needed
					product_data = {}
					product_resp = None
					raw_product = _safe_get(raw, "product", default={})
					
					# Check seller name FIRST from search results - skip API calls for Walmart.com
					search_seller_name = (
						_safe_get(raw, "offers", "primary", "seller", "name") or
						_safe_get(raw, "offers", "primary", "seller_name") or
						""
					).lower()
					is_walmart_seller = search_seller_name in ("walmart.com", "walmart", "walmart inc.")
					
					# Check seller ID from search results first
					search_seller_id = _safe_get(raw, "offers", "primary", "seller", "id") or _safe_get(raw, "offers", "primary", "seller_id")
					has_numeric_seller_id = search_seller_id and _is_numeric_string(str(search_seller_id))
					has_seller_url = _safe_get(raw, "offers", "primary", "seller", "url") or _safe_get(raw, "offers", "primary", "seller", "link")
					
					# OPTIMIZATION: Skip product API calls when possible
					# Key insight: If search results have seller URL, we can enrich via seller_profile API directly!
					# Only call Product API if we REALLY need it:
					# 1. Missing product data (sku/description/brand) AND missing seller URL
					# 2. OR missing seller URL AND we need numeric seller ID (but seller_profile accepts URL too!)
					needs_product_api_for_seller = False
					# Check if we have product data (sku/description/brand)
					has_product_data = raw_product and (raw_product.get("sku") or raw_product.get("description") or raw_product.get("brand"))
					# Check if we have UPC
					has_upc = raw_product and (raw_product.get("upc") or raw_product.get("gtin"))
					# Need Product API if missing product data OR missing UPC
					needs_product_api_for_data = not (has_product_data and has_upc)
					
					if retry_seller_passes > 0 and not is_walmart_seller:
						# OPTIMIZATION: If we have seller URL from search, skip Product API!
						# We can enrich seller directly via seller_profile(url=seller_url)
						if not has_seller_url:
							# Missing seller URL - need Product API to get it
							if not has_numeric_seller_id:
								# UUID seller ID + missing URL - need Product API for both
								needs_product_api_for_seller = True
								if debug:
									print(f"[{_ts()}]   UUID seller ID + missing URL - will call product API")
							else:
								# Numeric seller ID but missing URL - can construct URL, but Product API might have better data
								# Actually, we can construct URL from numeric ID: https://www.walmart.com/seller/{id}
								# So skip Product API if we have numeric ID!
								if debug:
									print(f"[{_ts()}]   Numeric seller ID - will construct URL, skipping product API")
						else:
							# We have seller URL! Skip Product API - can enrich via seller_profile(url)
							if debug:
								print(f"[{_ts()}]   ✅ Seller URL exists - skipping product API (will enrich via URL)")
					elif is_walmart_seller:
						if debug:
							print(f"[{_ts()}]   ⏭️  Walmart.com seller - skipping product API call")
					
					# COST OPTIMIZATION: Minimize Product API calls
					# Only call Product API if ABSOLUTELY necessary (missing critical product data)
					# Skip Product API if we have seller URL (can enrich via seller_profile if needed)
					# Skip Product API for Walmart.com sellers (no enrichment needed)
					
					# Check if search result already has sufficient product data AND UPC
					# BUT: Always call Product API if we need seller URL for UUID sellers OR missing UPC
					if has_product_data and has_upc and not needs_product_api_for_seller:
						# Use data from search results - skip API call to save costs
						# UNLESS we need seller URL for UUID sellers OR missing UPC
						product_data = normalize_product(raw_product)
						product_resp = None
						if debug:
							print(f"[{_ts()}]   ⚡ Using search results data - skipping product API (cost savings)")
					else:
						# Call Product API if:
						# 1. Missing critical product data OR missing UPC, OR
						# 2. Need seller URL for UUID sellers (needs_product_api_for_seller)
						if needs_product_api_for_data or needs_product_api_for_seller:
							try:
								if debug:
									if needs_product_api_for_seller:
										print(f"[{_ts()}]   Calling product API (UUID seller - need seller URL)")
									elif not has_upc:
										print(f"[{_ts()}]   Calling product API (missing UPC)")
									else:
										print(f"[{_ts()}]   Calling product API (missing product data)")
								product_resp = client.product(listing_id)
								if debug:
									write_debug_json(product_resp, f"debug_product_{listing_id}.json")
								product_data = normalize_product(product_resp.get("product") or product_resp)
								if debug:
									print(f"[{_ts()}]   Product API call successful")
							except Exception as e:
								if debug:
									print(f"[{_ts()}]   Product API call failed: {e}")
								product_resp = None
								product_data = normalize_product(raw_product) if raw_product else {}
						else:
							# Skip Product API - use search results only
							product_data = normalize_product(raw_product) if raw_product else {}
							product_resp = None
							if debug:
								print(f"[{_ts()}]   ⚡ Skipping product API (cost savings)")
					primary_offer = _safe_get(raw, "offers", "primary") or {}
					offers = [primary_offer] if primary_offer else []
					normalized_offers = [normalize_offer(o) for o in offers]
					# Derive primary seller details and try enrichment via BlueCart (US only), else product page scrape
					primary_o = normalized_offers[0] if normalized_offers else {}
					
					# IMPORTANT: Product API has better seller data (numeric ID and URL)
					# Check product API response for seller data first
					# Product API structure: product.buybox_winner.seller (NOT offers.primary.seller!)
					product_obj = product_resp.get("product") if isinstance(product_resp, dict) else (product_resp or {})
					product_buybox = _safe_get(product_obj, "buybox_winner") or {}
					product_offer = _safe_get(product_resp or {}, "offers", "primary") or product_buybox or {}
					product_seller = _safe_get(product_buybox, "seller") or _safe_get(product_offer, "seller") or {}
					
					# Get seller URL from multiple sources (product API has best data)
					seller_url_from_offer = (
						_safe_get(product_seller, "link") or  # Product API: offers.primary.seller.link
						_safe_get(product_offer, "seller", "link") or
						primary_o.get("url") or 
						_safe_get(primary_offer, "seller", "url") or 
						_safe_get(primary_offer, "seller_url") or
						_safe_get(primary_offer, "url") or
						_safe_get(raw, "offers", "primary", "seller", "url") or
						_safe_get(raw, "offers", "primary", "url")
					)
					
					# Get seller ID from multiple sources (product API has numeric ID)
					# Product API has: offers.primary.seller.id (numeric) and id_secondary (UUID)
					seller_id = (
						_safe_get(product_seller, "id") or  # Product API: offers.primary.seller.id (NUMERIC!)
						_safe_get(product_offer, "seller", "id") or
						primary_o.get("seller_id") or 
						_safe_get(primary_offer, "seller", "id") or 
						_safe_get(primary_offer, "seller_id") or
						_collect_numeric_seller_id(raw) or
						_collect_numeric_seller_id(product_resp or {})
					)
					
					# Try to extract numeric seller ID from seller URL if available
					# Pattern: https://www.walmart.com/seller/{numeric_id}
					if seller_url_from_offer and "/seller/" in seller_url_from_offer:
						try:
							url_parts = seller_url_from_offer.split("/seller/")
							if len(url_parts) > 1:
								potential_id = url_parts[1].split("/")[0].split("?")[0].strip()
								if _is_numeric_string(potential_id):
									seller_id = potential_id  # Use numeric ID from URL
						except Exception:
							pass
					
					# REMOVED: offers API call - too slow, causes 10+ min delays per item
					# Seller URL will be constructed from seller_id or from seller enrichment later
					# If still no URL, construct it from seller_id (don't call API here - too slow)
					# Seller enrichment will get the URL later if needed
					if not seller_url_from_offer and seller_id and _is_numeric_string(str(seller_id)):
						seller_url_from_offer = f"https://www.walmart.com/seller/{seller_id}"
					enriched: Dict[str, Any] = {}
					# Try BlueCart seller_profile using numeric seller_id if present anywhere in raw/product payloads
					numeric_sid: Optional[str] = None
					if primary_o.get("seller_id") and _is_numeric_string(str(primary_o.get("seller_id"))):
						numeric_sid = str(primary_o.get("seller_id"))
					else:
						# search the raw search item for a numeric seller id
						numeric_sid = _collect_numeric_seller_id(raw) or _collect_numeric_seller_id(product_resp or {})
					# Extract seller name from multiple sources (needed for debug and later use)
					seller_name = (
						primary_o.get("seller_name") or
						_safe_get(primary_offer, "seller", "name") or
						_safe_get(primary_offer, "seller_name") or
						_safe_get(raw, "offers", "primary", "seller", "name") or
						_safe_get(raw, "offers", "primary", "seller_name") or
						"Unknown Seller"
					)
					# DISABLED: Seller enrichment removed due to API timeouts
					# Just use basic seller info from search results - no enrichment needed
					# Optional debug for each collected item
					if debug:
						seller_url = seller_url_from_offer or primary_o.get("url") or "N/A"
						print(f"[{_ts()}]   Seller debug listing_id={listing_id} seller_id={seller_id} name={seller_name} url={seller_url}")
					# Debug: Log when we find seller IDs but no URLs (to help identify which sellers have IDs)
					if seller_id and not seller_url_from_offer:
						if debug:
							print(f"[{_ts()}]   ⚠️ Found seller_id={seller_id} for seller={seller_name} but no URL constructed!")
					# Debug: Log seller ID extraction for non-Walmart sellers to see what we're getting
					# This helps identify which sellers have IDs and which don't
					if seller_name and seller_name.lower() not in ("walmart.com", "walmart", "walmart inc."):
						if seller_id and seller_url_from_offer:
							# Success case - we found both ID and URL
							if debug:
								print(f"[{_ts()}]   ✅ Seller={seller_name} has seller_id={seller_id} and URL={seller_url_from_offer[:50]}...")
						elif seller_id and not seller_url_from_offer:
							# We have ID but didn't construct URL - this shouldn't happen with current logic
							print(f"[{_ts()}]   ⚠️  Found seller_id={seller_id} for seller={seller_name} but no URL constructed (listing_id={listing_id})")
						elif not seller_id and not seller_url_from_offer:
							# No ID found - API limitation
							if debug:
								print(f"[{_ts()}]   ❌ No seller_id found for seller={seller_name} (listing_id={listing_id}) - API doesn't provide it")
					# Store history
					insert_listing_snapshot(listing_id, {
						"keyword": kw,
						"listing": listing,
						"product": product_data,
						"offers": normalized_offers,
					})
					for o in normalized_offers:
						if o.get("seller_id"):
							insert_seller_snapshot(listing_id, str(o["seller_id"]), o)
						if offers_export:
							# Try to enrich seller details by visiting the product page and discovering the seller profile URL
							# Simplified: skip seller profile enrichment for now
							offer_enriched = {}
							row = {
								"keyword": kw,
								"listing_id": listing_id,
								"seller_name": o.get("seller_name"),
								"seller_profile_picture": offer_enriched.get("seller_profile_picture"),
								"seller_profile_url": offer_enriched.get("seller_profile_url") or o.get("url"),
								"seller_rating": o.get("seller_rating"),
								"total_reviews": o.get("total_reviews"),
								"price": o.get("price"),
								"currency": o.get("currency"),
								"email_address": offer_enriched.get("email_address"),
								"business_legal_name": offer_enriched.get("business_legal_name"),
								"country": offer_enriched.get("country"),
								"state_province": offer_enriched.get("state_province"),
								"zip_code": offer_enriched.get("zip_code"),
								"phone_number": offer_enriched.get("phone_number"),
								"address": offer_enriched.get("address"),
							}
							all_offers.append(row)
					# Accumulate for export (Listings schema)
					images_list = product_data.get("images") or ([listing.get("image")] if listing.get("image") else [])
					images_joined = "|".join(images_list) if images_list else None
					in_stock = _safe_get(raw, "inventory", "in_stock")
					# Extract units_available from offers, inventory, or product data
					# Try multiple sources, but don't default to 1 - leave as None if not found
					units_available = None
					if primary_o.get("quantity") is not None:
						units_available = primary_o.get("quantity")
					elif _safe_get(raw, "inventory", "quantity") is not None:
						units_available = _safe_get(raw, "inventory", "quantity")
					elif _safe_get(raw, "inventory", "available_quantity") is not None:
						units_available = _safe_get(raw, "inventory", "available_quantity")
					elif _safe_get(product_resp or {}, "product", "inventory", "quantity") is not None:
						units_available = _safe_get(product_resp or {}, "product", "inventory", "quantity")
					elif _safe_get(product_resp or {}, "product", "inventory", "available_quantity") is not None:
						units_available = _safe_get(product_resp or {}, "product", "inventory", "available_quantity")
					# If still None, leave it as None (don't default to 1)
					# seller_name already extracted above (before debug section)
					# Check if seller is Walmart by comparing seller names (handle None safely)
					_seller_name_from_offer = _safe_get(primary_offer, "seller", "name", default="") or ""
					_seller_name_from_primary_o = primary_o.get("seller_name", "") or ""
					_is_walmart = False
					if _seller_name_from_primary_o and isinstance(_seller_name_from_primary_o, str):
						_is_walmart = _seller_name_from_primary_o.lower() in ("walmart.com", "walmart", "walmart inc.")
					if not _is_walmart and _seller_name_from_offer and isinstance(_seller_name_from_offer, str):
						_is_walmart = _seller_name_from_offer.lower() in ("walmart.com", "walmart", "walmart inc.")
					# DATA QUALITY: Validate seller URL
					validated_seller_url, is_valid_url = validate_seller_url(seller_url_from_offer, seller_id, seller_name)
					if not is_valid_url and seller_id and _is_numeric_string(str(seller_id)):
						validated_seller_url = f"https://www.walmart.com/seller/{seller_id}"
						is_valid_url = True
					final_seller_url = validated_seller_url if is_valid_url else (seller_url_from_offer or "")
					
					# DATA QUALITY: Price validation - filter out invalid prices
					product_price = listing.get("price")
					is_price_valid, price_info = validate_price_and_stock(product_price, units_available)
					if not is_price_valid:
						if debug:
							print(f"[{_ts()}]   ❌ Price validation: Skipping product with invalid price (price: {product_price}, status: {price_info.get('stock_status')})", flush=True)
						continue
					
					# DATA QUALITY: Enhanced UPC collection from multiple sources
					enhanced_upc = collect_upc_from_multiple_sources(raw_product, product_resp, raw)
					if not enhanced_upc:
						# Fallback to existing UPC collection
						enhanced_upc = listing.get("upc") or product_data.get("upc")
					
					# Phase 2: Extract additional product fields
					product_category = product_data.get("category") or ""
					product_dimensions = product_data.get("dimensions") or ""
					product_weight = product_data.get("weight") or ""
					product_reviews_count = product_data.get("product_reviews_count") or 0
					product_rating = product_data.get("product_rating")
					shipping_cost = product_data.get("shipping_cost") or ""
					estimated_delivery = product_data.get("estimated_delivery") or ""
					product_variants = product_data.get("variants")
					
					# Format variants as string if available
					variants_str = ""
					if product_variants and isinstance(product_variants, list):
						variant_strings = []
						for v in product_variants:
							if isinstance(v, dict):
								variant_parts = []
								if v.get("title"):
									variant_parts.append(f"Title: {v.get('title')}")
								if v.get("price"):
									variant_parts.append(f"Price: ${v.get('price')}")
								if v.get("sku"):
									variant_parts.append(f"SKU: {v.get('sku')}")
								if variant_parts:
									variant_strings.append(" | ".join(variant_parts))
						if variant_strings:
							variants_str = " || ".join(variant_strings)
					
					combined = {
						"keyword": kw,
						"listing_id": listing_id,
						"listing_title": listing.get("title"),
						"product_images": images_joined,
						"product_sku": product_data.get("sku"),
							"item_number": listing_id,
							"price": price_info.get("price"),  # Use validated price
							"currency": listing.get("currency"),
							"units_available": units_available if price_info.get("stock_status") == "In Stock" else None,  # Only set if in stock
							"stock_status": price_info.get("stock_status"),  # Add stock status field
							"in_stock": in_stock,
							"brand": listing.get("brand") or product_data.get("brand"),
						"asin": listing.get("asin") or product_data.get("asin"),
						"upc": enhanced_upc,  # Use enhanced UPC collection
						"walmart_id": listing_id,
							"listing_url": listing.get("url"),
							"full_product_description": product_data.get("description"),
							# Phase 2: Additional product fields
							"product_category": product_category,
							"product_dimensions": product_dimensions,
							"product_weight": product_weight,
							"product_reviews_count": product_reviews_count,
							"product_rating": product_rating,
							"shipping_cost": shipping_cost,
							"estimated_delivery": estimated_delivery,
							"product_variants": variants_str,
							# Seller fields (from primary offer - will be enriched later)
							"seller_name": seller_name,
							"seller_profile_url": final_seller_url,  # Use validated URL
							"seller_rating": primary_o.get("seller_rating") or _safe_get(primary_offer, "seller", "rating"),
							"total_reviews": primary_o.get("total_reviews") or _safe_get(primary_offer, "seller", "reviews_count"),
							# Set Walmart contact info if seller is Walmart
							"email_address": "help@walmart.com" if _is_walmart else "",
							"business_legal_name": "Walmart Inc." if _is_walmart else "",
							"phone_number": "1-800-925-6278" if _is_walmart else "",
							"address": "702 SW 8th St, Bentonville, AR 72716, USA" if _is_walmart else "",
							"country": "",
							"state_province": "",
							"zip_code": "",
							"offers_count": len(normalized_offers),
							# Internal keys for seller enrichment tracking
							"_primary_seller_id": seller_id,
							"_primary_seller_url": final_seller_url,
					}
					all_records.append(combined)
					
					# Track sellers for enrichment if they're missing URL, email, or phone (and not Walmart)
					# IMPORTANT: seller_profile API accepts BOTH numeric seller IDs AND seller URLs
					# So we can enrich sellers even if we only have UUID seller IDs (use seller URL instead)
					if not _is_walmart and retry_seller_passes > 0:
						needs_enrichment = False
						if not final_seller_url:
							needs_enrichment = True  # Missing URL
						if not combined.get("email_address") and not combined.get("phone_number"):
							needs_enrichment = True  # Missing contact info
						
						# Add to pending if we have either:
						# 1. Numeric seller ID (preferred)
						# 2. Seller URL (works even with UUID seller IDs!)
						if needs_enrichment:
							if seller_id and _is_numeric_string(str(seller_id)):
								# Use numeric seller ID (best option)
								pending_sellers.append((str(seller_id), final_seller_url))
							elif final_seller_url:
								# Use seller URL (works even if seller_id is UUID!)
								# Pass None for seller_id, URL for url parameter
								pending_sellers.append((None, final_seller_url))
					collected += 1
					items_added_this_page += 1
					# Show progress every 5 items or on first item
					if collected % 5 == 0 or collected == 1:
						max_display = max_per_keyword if max_per_keyword > 0 else "unlimited"
						print(f"[{_ts()}]   Collected {collected}/{max_display} items for '{kw}'", flush=True)
				
				# If we got 0 new items this page (all duplicates), stop pagination
				if items_added_this_page == 0 and page > 1:
					print(f"[{_ts()}]   No new items on page {page} (all duplicates); stopping pagination", flush=True)
					break
				
				page += 1
			print(f"[{_ts()}] Keyword done: {kw} | collected={collected} items total", flush=True)

		# Retry pass for seller_profile if requested (US only)
		if retry_seller_passes > 0 and client.site == "walmart.com" and pending_sellers:
			print(f"[{_ts()}] 🔄 Starting seller enrichment pass | pending sellers: {len(pending_sellers)}", flush=True)
			# unique pending set
			unique_pending: List[Tuple[Optional[str], Optional[str]]] = []
			seen_keys: set = set()
			for sid, surl in pending_sellers:
				key = f"sid:{sid}" if sid else f"url:{surl}"
				if key in seen_keys:
					continue
				seen_keys.add(key)
				unique_pending.append((sid, surl))
			print(f"[{_ts()}] 🔄 Unique sellers to enrich: {len(unique_pending)}", flush=True)
			# OPTIMIZATION: Use parallel seller enrichment for speed
			from concurrent.futures import ThreadPoolExecutor, as_completed
			max_workers = 5  # Parallel enrichment (5 concurrent API calls)
			
			def enrich_single_seller(seller_data: Tuple[int, Optional[str], Optional[str]]) -> Tuple[int, Optional[str], Optional[str], Optional[Dict[str, Any]]]:
				"""Enrich a single seller - designed for parallel execution"""
				idx, sid, surl = seller_data
				key = f"sid:{sid}" if sid else f"url:{surl}"
				if key in seller_cache:
					return (idx, sid, surl, None)  # Already cached
				try:
					sp = client.seller_profile(seller_id=sid, url=surl)
					# Check for API errors in response
					if isinstance(sp, dict):
						request_info = sp.get("request_info")
						if request_info:
							msg = request_info.get("message", "")
							status = request_info.get("status", "")
							if status and status != "success":
								if debug:
									print(f"[{_ts()}]   ⚠️ API warning for seller {idx}: {msg}", flush=True)
					fields = _extract_seller_fields(sp)
					# Cache seller data if we got any useful fields (email, phone, URL, etc.)
					if fields.get("email_address") or fields.get("phone_number") or fields.get("seller_profile_url"):
						return (idx, sid, surl, fields)
					else:
						return (idx, sid, surl, None)  # No useful data
				except Exception as e:
					if debug:
						error_type = type(e).__name__
						print(f"[{_ts()}]   ⚠️ Error enriching seller {idx} ({sid or surl[:30] if surl else 'N/A'}): {error_type}", flush=True)
					return (idx, sid, surl, None)  # Error - return None
			
			for attempt in range(retry_seller_passes):
				print(f"[{_ts()}] 🔄 Seller enrichment pass {attempt+1}/{retry_seller_passes} | processing {len(unique_pending)} sellers (parallel: {max_workers} workers)", flush=True)
				still_pending: List[Tuple[Optional[str], Optional[str]]] = []
				enriched_count = 0
				
				# Prepare seller data with indices for tracking
				seller_data_list = [(idx, sid, surl) for idx, (sid, surl) in enumerate(unique_pending, 1)]
				
				# Filter out already cached sellers
				sellers_to_enrich = [(idx, sid, surl) for idx, sid, surl in seller_data_list 
									 if (f"sid:{sid}" if sid else f"url:{surl}") not in seller_cache]
				
				if not sellers_to_enrich:
					print(f"[{_ts()}]   ✅ All sellers already cached - skipping enrichment", flush=True)
					break
				
				# Parallel enrichment
				completed = 0
				with ThreadPoolExecutor(max_workers=max_workers) as executor:
					future_to_seller = {executor.submit(enrich_single_seller, seller_data): seller_data for seller_data in sellers_to_enrich}
					
					for future in as_completed(future_to_seller):
						completed += 1
						idx, sid, surl, fields = future.result()
						key = f"sid:{sid}" if sid else f"url:{surl}"
						
						if fields:
							seller_cache[key] = fields
							enriched_count += 1
							if completed % 5 == 0 or completed == len(sellers_to_enrich):
								print(f"[{_ts()}]   ✅ Enriched {completed}/{len(sellers_to_enrich)} sellers", flush=True)
						else:
							still_pending.append((sid, surl))
						
						# Small delay to avoid rate limiting (distributed across parallel calls)
						if retry_seller_delay > 0 and completed < len(sellers_to_enrich):
							time.sleep(retry_seller_delay / max_workers)
				
				print(f"[{_ts()}] ✅ Pass {attempt+1} complete: enriched {enriched_count} sellers, {len(still_pending)} still pending", flush=True)
				unique_pending = still_pending
				if not unique_pending:
					print(f"[{_ts()}] ✅ All sellers enriched!", flush=True)
					break
			print(f"[{_ts()}] ✅ Seller enrichment complete", flush=True)
			# Reconcile results into all_records - update with enriched seller data
			for r in all_records:
				key = None
				sid = r.get("_primary_seller_id")
				surl = r.get("_primary_seller_url")
				if sid:
					key = f"sid:{sid}"
				elif surl:
					key = f"url:{surl}"
				if key and key in seller_cache:
					fields = seller_cache[key]
					# Update seller fields from enriched data
					for k in ("email_address", "phone_number", "address", "country", "state_province", "zip_code", "seller_profile_picture", "seller_profile_url", "business_legal_name", "seller_rating", "total_reviews"):
						if fields.get(k):
							# Always update seller_profile_url if available from enrichment (it's more complete)
							if k == "seller_profile_url" and fields.get(k):
								r["seller_profile_url"] = fields[k]
							# For other fields, only update if not already set
							elif not r.get(k):
								r[k] = fields[k]

		# export - include keyword in filename if single keyword
		if len(keyword_list) == 1:
			safe_keyword = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in keyword_list[0])
			safe_keyword = safe_keyword.replace(' ', '_')
			name_prefix = f"walmart_scan_{safe_keyword}"
		else:
			name_prefix = "walmart_scan"
		# Clean up records: drop empties and dedupe by (listing_id, keyword)
		seen_keys: set = set()
		cleaned_records: List[Dict[str, Any]] = []
		for r in all_records:
			listing_id = r.get("listing_id")
			keyword = r.get("keyword")
			if not listing_id or not r.get("listing_title"):
				continue
			# Deduplicate by (listing_id, keyword) - same product can appear in different keywords
			key = (str(listing_id), str(keyword) if keyword else "")
			if key in seen_keys:
				continue
			seen_keys.add(key)
			# strip internal keys
			r2 = {k: v for k, v in r.items() if not k.startswith("_")}
			cleaned_records.append(r2)

		seen_offers: set = set()
		cleaned_offers: List[Dict[str, Any]] = []
		for r in all_offers:
			key = (r.get("listing_id"), r.get("seller_name"), r.get("price"))
			if not r.get("listing_id"):
				continue
			if key in seen_offers:
				continue
			seen_offers.add(key)
			cleaned_offers.append(r)

		print(f"[{_ts()}] ✅ Records cleaned: {len(cleaned_records)} total records ready for export")

		# Verify we have records before attempting export
		if not cleaned_records:
			print(f"[{_ts()}] WARNING: No cleaned records to export. Total collected: {len(all_records)}")
			return

		if "json" in export:
			if ENHANCED_EXPORTS_AVAILABLE:
				# Use enhanced exporters with integration format
				domain = client.site if hasattr(client, 'site') else 'walmart.com'
				json_path = export_json_enhanced(cleaned_records, name_prefix, domain)
				print(f"[{_ts()}] Enhanced JSON exported: {json_path}")
			else:
				json_path = export_json(cleaned_records, name_prefix)
				print(f"[{_ts()}] JSON exported: {json_path}")
		if "csv" in export:
			try:
				print(f"[{_ts()}] Starting CSV export for {len(cleaned_records)} records...")
				if ENHANCED_EXPORTS_AVAILABLE:
					# Use enhanced exporters with integration format
					domain = client.site if hasattr(client, 'site') else 'walmart.com'
					print(f"[{_ts()}] Using enhanced CSV exporter with domain: {domain}")
					csv_path = export_csv_enhanced(cleaned_records, name_prefix, domain=domain)
					print(f"[{_ts()}] ✅ Enhanced CSV exported: {csv_path}")
				else:
					print(f"[{_ts()}] Using standard CSV exporter")
					csv_path = export_csv(cleaned_records, name_prefix)
					print(f"[{_ts()}] ✅ CSV exported: {csv_path}")
			except Exception as e:
				print(f"[{_ts()}] ❌ ERROR exporting CSV: {e}")
				import traceback
				traceback.print_exc()
				raise
		if offers_export and cleaned_offers:
			if "json" in export:
				o_json_path = export_json(cleaned_offers, name_prefix + "_offers")
				print(f"[{_ts()}] Offers JSON exported: {o_json_path}")
			if "csv" in export:
				o_csv_path = export_csv(cleaned_offers, name_prefix + "_offers")
				print(f"[{_ts()}] Offers CSV exported: {o_csv_path}")
		print(f"[{_ts()}] ✅✅✅ Scraping and export completed successfully! ✅✅✅")
		
	except Exception as e:
		print(f"[{_ts()}] ❌❌❌ FATAL ERROR in run() function: {e}")
		import traceback
		traceback.print_exc()
		raise


def main(args=None):
	parser = argparse.ArgumentParser(description="Walmart scraper via BlueCart API")
	parser.add_argument("--keywords", type=str, default="", help="Comma-separated keywords")
	parser.add_argument("--keywords-file", type=str, default="", help="Path to file with one keyword per line")
	parser.add_argument("--max-per-keyword", type=int, default=10)
	parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between API calls")
	parser.add_argument("--export", nargs="+", default=["json", "csv"], choices=["json", "csv"])
	parser.add_argument("--offers-export", action="store_true", help="Export per-offer dataset in addition to listings")
	parser.add_argument("--max-pages", type=int, default=50, help="Max pages to paginate for search (0 or negative = unlimited)")
	parser.add_argument("--debug", action="store_true", help="Write raw API responses to output/debug files for troubleshooting")
	parser.add_argument("--walmart-domain", type=str, default="", help="Walmart domain (e.g., walmart.com, walmart.ca, walmart.com.mx)")
	parser.add_argument("--category-id", type=str, default="", help="Walmart category id to filter search")
	parser.add_argument("--retry-seller-passes", type=int, default=0, help="Retry passes for seller_profile after crawl")
	parser.add_argument("--retry-seller-delay", type=float, default=15.0, help="Seconds to distribute across each retry pass")
	args = parser.parse_args(args)

	keywords: List[str] = []
	if args.keywords:
		keywords.extend([s.strip() for s in args.keywords.split(",") if s.strip()])
	if args.keywords_file:
		with open(args.keywords_file, "r", encoding="utf-8") as f:
			keywords.extend([line.strip() for line in f if line.strip()])
	if not keywords:
		raise SystemExit("No keywords provided. Use --keywords or --keywords-file")

	walmart_domain = args.walmart_domain.strip() or None
	category_id = args.category_id.strip() or None
	run(keywords, args.max_per_keyword, args.export, args.sleep, args.offers_export, args.max_pages, args.debug, walmart_domain, category_id, args.retry_seller_passes, args.retry_seller_delay)


if __name__ == "__main__":
	main()


