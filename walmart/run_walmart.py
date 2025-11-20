import argparse
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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
	return {
		"listing_id": _safe_get(item, "item_id") or _safe_get(item, "product_id") or _safe_get(item, "product", "item_id") or _safe_get(item, "product", "product_id"),
		"sku": _safe_get(item, "sku") or _safe_get(item, "product", "sku"),
		"title": _safe_get(item, "title") or _safe_get(item, "product", "title"),
		"brand": _safe_get(item, "brand") or _safe_get(item, "product", "brand"),
		"description": _safe_get(item, "description") or _safe_get(item, "product", "description"),
		"images": coerced_images,
		"asin": _safe_get(item, "asin") or _safe_get(item, "product", "asin"),
		"upc": _safe_get(item, "upc") or _safe_get(item, "product", "upc") or _safe_get(item, "product", "gtin"),
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
					
					seen_this_keyword.add(listing_id)
					upsert_listing_summary(listing_id, listing.get("listing_title"), listing.get("brand"), listing.get("url"))
					# OPTIMIZATION: Try to use product data from search results first, only call API if needed
					product_data = {}
					product_resp = None
					raw_product = _safe_get(raw, "product", default={})
					# Check if search result already has sufficient product data
					if raw_product and (raw_product.get("sku") or raw_product.get("description") or raw_product.get("brand")):
						# Use data from search results - skip API call for speed
						product_data = normalize_product(raw_product)
					else:
						# Only call API if search results don't have product data
						try:
							product_resp = client.product(listing_id)
							if debug:
								write_debug_json(product_resp, f"debug_product_{listing_id}.json")
							product_data = normalize_product(product_resp.get("product") or product_resp)
						except Exception:
							product_resp = None
							product_data = normalize_product(raw_product)
					primary_offer = _safe_get(raw, "offers", "primary") or {}
					offers = [primary_offer] if primary_offer else []
					normalized_offers = [normalize_offer(o) for o in offers]
					# Derive primary seller details and try enrichment via BlueCart (US only), else product page scrape
					primary_o = normalized_offers[0] if normalized_offers else {}
					# Try to get seller URL from offers API if not found in search results
					seller_url_from_offer = (
						primary_o.get("url") or 
						_safe_get(primary_offer, "seller", "url") or 
						_safe_get(primary_offer, "seller_url") or
						_safe_get(primary_offer, "url") or
						_safe_get(raw, "offers", "primary", "seller", "url") or
						_safe_get(raw, "offers", "primary", "url")
					)
					# Get seller ID from multiple sources for URL construction
					seller_id = (
						primary_o.get("seller_id") or 
						_safe_get(primary_offer, "seller", "id") or 
						_safe_get(primary_offer, "seller_id") or
						_collect_numeric_seller_id(raw) or
						_collect_numeric_seller_id(product_resp or {})
					)
					# If no seller URL found, try offers API (always try if no URL, even if we have seller_id)
					if not seller_url_from_offer:
						try:
							offers_resp = client.offers(listing_id, page=1)
							if offers_resp and isinstance(offers_resp, dict):
								# Try to extract seller ID from entire offers response (if not already found)
								if not seller_id:
									seller_id = _collect_numeric_seller_id(offers_resp)
								offers_list = offers_resp.get("offers") or offers_resp.get("data", {}).get("offers") or []
								if offers_list and len(offers_list) > 0:
									first_offer = offers_list[0] if isinstance(offers_list[0], dict) else {}
									# Try to get seller URL from offer
									seller_url_from_offer = (
										_safe_get(first_offer, "seller", "url") or
										_safe_get(first_offer, "seller_url") or
										_safe_get(first_offer, "url") or
										first_offer.get("seller_url") or
										first_offer.get("url") or
										seller_url_from_offer  # Keep existing if found
									)
									# Try to get seller ID from offer if not already found
									if not seller_id:
										seller_id = (
											_safe_get(first_offer, "seller", "id") or
											_safe_get(first_offer, "seller_id") or
											first_offer.get("seller_id") or
											(first_offer.get("seller", {}).get("id") if isinstance(first_offer.get("seller"), dict) else None) or
											_collect_numeric_seller_id(first_offer)
										)
						except Exception:
							pass
					# If still no URL, try to get it from BlueCart seller_profile API using seller ID
					if not seller_url_from_offer and seller_id:
						seller_id_str = str(seller_id).strip()
						if seller_id_str:
							try:
								# Call BlueCart seller_profile API to get seller URL
								seller_profile_resp = client.seller_profile(seller_id=seller_id_str)
								if seller_profile_resp and isinstance(seller_profile_resp, dict):
									# Extract seller URL from seller_profile response (checks multiple fields)
									seller_url_from_offer = (
										_safe_get(seller_profile_resp, "seller_url") or
										_safe_get(seller_profile_resp, "url") or
										_safe_get(seller_profile_resp, "seller_details", "seller_url") or
										_safe_get(seller_profile_resp, "seller_details", "url") or
										_safe_get(seller_profile_resp, "seller", "url") or
										seller_profile_resp.get("seller_url") or
										seller_profile_resp.get("url")
									)
							except Exception:
								# If API call fails, just leave it empty (don't construct URL)
								pass
					enriched: Dict[str, Any] = {}
					# Try BlueCart seller_profile using numeric seller_id if present anywhere in raw/product payloads
					numeric_sid: Optional[str] = None
					if primary_o.get("seller_id") and _is_numeric_string(str(primary_o.get("seller_id"))):
						numeric_sid = str(primary_o.get("seller_id"))
					else:
						# search the raw search item for a numeric seller id
						numeric_sid = _collect_numeric_seller_id(raw) or _collect_numeric_seller_id(product_resp or {})
					# DISABLED: Seller enrichment removed due to API timeouts
					# Just use basic seller info from search results - no enrichment needed
					# Optional debug for each collected item
					if debug:
						seller_name = primary_o.get("seller_name") or _safe_get(primary_offer, "seller", "name") or "Unknown"
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
					# Extract seller name from multiple sources
					seller_name = (
						primary_o.get("seller_name") or
						_safe_get(primary_offer, "seller", "name") or
						_safe_get(primary_offer, "seller_name") or
						_safe_get(raw, "offers", "primary", "seller", "name") or
						_safe_get(raw, "offers", "primary", "seller_name") or
						"Unknown Seller"
					)
					# Check if seller is Walmart by comparing seller names (handle None safely)
					_seller_name_from_offer = _safe_get(primary_offer, "seller", "name", default="") or ""
					_seller_name_from_primary_o = primary_o.get("seller_name", "") or ""
					_is_walmart = False
					if _seller_name_from_primary_o and isinstance(_seller_name_from_primary_o, str):
						_is_walmart = _seller_name_from_primary_o.lower() in ("walmart.com", "walmart", "walmart inc.")
					if not _is_walmart and _seller_name_from_offer and isinstance(_seller_name_from_offer, str):
						_is_walmart = _seller_name_from_offer.lower() in ("walmart.com", "walmart", "walmart inc.")
					combined = {
						"keyword": kw,
						"listing_id": listing_id,
						"listing_title": listing.get("title"),
						"product_images": images_joined,
						"product_sku": product_data.get("sku"),
							"item_number": listing_id,
							"price": listing.get("price"),
							"currency": listing.get("currency"),
							"units_available": units_available,
							"in_stock": in_stock,
							"brand": listing.get("brand") or product_data.get("brand"),
						"asin": listing.get("asin") or product_data.get("asin"),
						"upc": listing.get("upc") or product_data.get("upc"),
						"walmart_id": listing_id,
							"listing_url": listing.get("url"),
							"full_product_description": product_data.get("description"),
							# Seller fields (from primary offer - no enrichment to avoid timeouts)
							"seller_name": seller_name,
							# Seller URL (already extracted above from multiple sources including offers API and constructed from seller ID)
							"seller_profile_url": seller_url_from_offer or "",
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
					}
					all_records.append(combined)
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
			for attempt in range(retry_seller_passes):
				print(f"[{_ts()}] 🔄 Seller enrichment pass {attempt+1}/{retry_seller_passes} | processing {len(unique_pending)} sellers", flush=True)
				still_pending: List[Tuple[Optional[str], Optional[str]]] = []
				enriched_count = 0
				for idx, (sid, surl) in enumerate(unique_pending, 1):
					key = f"sid:{sid}" if sid else f"url:{surl}"
					if key in seller_cache:
						continue
					try:
						# Show progress more frequently (every 5 sellers or first/last)
						if idx % 5 == 0 or idx == 1 or idx == len(unique_pending):
							print(f"[{_ts()}]   Enriching seller {idx}/{len(unique_pending)}: {sid or (surl[:50] if surl else 'N/A')}", flush=True)
						sp = client.seller_profile(seller_id=sid, url=surl)
						# Check for API errors in response
						if isinstance(sp, dict):
							request_info = sp.get("request_info")
							if request_info:
								msg = request_info.get("message", "")
								status = request_info.get("status", "")
								if status and status != "success":
									print(f"[{_ts()}]   ⚠️ API warning for seller {idx}: {msg}", flush=True)
						fields = _extract_seller_fields(sp)
						if fields.get("email_address") or fields.get("phone_number"):
							seller_cache[key] = fields
							enriched_count += 1
							if idx % 5 == 0:
								print(f"[{_ts()}]   ✅ Seller {idx} enriched successfully", flush=True)
						else:
							if idx % 10 == 0:
								print(f"[{_ts()}]   ⚠️ Seller {idx} has no email/phone - skipping", flush=True)
							still_pending.append((sid, surl))
					except Exception as e:
						error_type = type(e).__name__
						error_msg = str(e)
						# Handle timeout errors specifically
						if "Timeout" in error_type or "timeout" in error_msg.lower():
							print(f"[{_ts()}]   ⏱️ Timeout enriching seller {idx}/{len(unique_pending)}: {sid or (surl[:50] if surl else 'N/A')} - skipping", flush=True)
						else:
							print(f"[{_ts()}]   ❌ Error enriching seller {idx}/{len(unique_pending)}: {error_type}: {error_msg[:100]}", flush=True)
							if idx <= 3:  # Show full traceback for first 3 non-timeout errors
								import traceback
								traceback.print_exc()
						still_pending.append((sid, surl))
					# pace requests across the delay window (minimal delay for speed)
					delay = max(0.02, retry_seller_delay / max(1, len(unique_pending)))
					time.sleep(delay)
				print(f"[{_ts()}] ✅ Pass {attempt+1} complete: enriched {enriched_count} sellers, {len(still_pending)} still pending", flush=True)
				unique_pending = still_pending
				if not unique_pending:
					print(f"[{_ts()}] ✅ All sellers enriched!", flush=True)
					break
			print(f"[{_ts()}] ✅ Seller enrichment complete", flush=True)
			# Reconcile results into all_records
			for r in all_records:
				if r.get("email_address") or r.get("phone_number"):
					continue
				key = None
				sid = r.get("_primary_seller_id")
				surl = r.get("_primary_seller_url")
				if sid:
					key = f"sid:{sid}"
				elif surl:
					key = f"url:{surl}"
				if key and key in seller_cache:
					fields = seller_cache[key]
					for k in ("email_address", "phone_number", "address", "country", "state_province", "zip_code", "seller_profile_picture", "seller_profile_url", "business_legal_name", "seller_rating", "total_reviews"):
						if fields.get(k) and not r.get(k):
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


