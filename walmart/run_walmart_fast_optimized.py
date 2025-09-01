import argparse
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bluecart_client import BlueCartClient
from config import get_config
from storage import init_db, insert_listing_snapshot, insert_seller_snapshot, upsert_listing_summary
from exporters import export_json, export_csv, write_debug_json


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
	"""Normalize BlueCart seller_profile response to our unified seller fields."""
	if not isinstance(sp, dict):
		return {}
	
	# Check if request failed
	request_info = sp.get("request_info", {})
	if not request_info.get("success", False):
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
			addr_obj.get("zipcode") or addr_obj.get("postalCode") or addr_obj.get("zip"),
			addr_obj.get("country") or addr_obj.get("addressCountry"),
		]
		address_text = address_text or " ".join([p for p in parts if p])
		country = (addr_obj.get("country") or addr_obj.get("addressCountry")) if not isinstance(addr_obj.get("addressCountry"), dict) else None
		state_province = addr_obj.get("state") or addr_obj.get("addressRegion")
		zip_code = addr_obj.get("zipcode") or addr_obj.get("postalCode") or addr_obj.get("zip")
	
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
	except Exception:
		pass
	return None


def normalize_listing_from_search(raw: Dict[str, Any]) -> Dict[str, Any]:
	"""Normalize BlueCart search response to our unified listing fields."""
	product = raw.get("product") or {}
	return {
		"listing_id": product.get("item_id") or raw.get("item_id") or raw.get("id"),
		"listing_title": product.get("title") or raw.get("title"),
		"listing_url": product.get("link") or product.get("url") or raw.get("url"),
		"brand": product.get("brand") or raw.get("brand"),
		"product_sku": product.get("sku") or raw.get("sku"),
		"full_product_description": product.get("description") or raw.get("description"),
		"product_images": product.get("images") or raw.get("images"),
		"in_stock": raw.get("in_stock", True),
		"price": _safe_get(raw, "offers", "primary", "price"),
		"currency": _safe_get(raw, "offers", "primary", "currency_symbol", default="USD"),
		"offers_count": len(raw.get("offers", [])),
		"units_available": raw.get("units_available"),
		"total_reviews": raw.get("total_reviews"),
		"seller_name": _safe_get(raw, "offers", "primary", "seller", "name"),
		"seller_id": _safe_get(raw, "offers", "primary", "seller", "id"),
		"seller_profile_url": _safe_get(raw, "offers", "primary", "seller", "url"),
		"seller_rating": raw.get("rating"),
	}


def normalize_product(product: Dict[str, Any]) -> Dict[str, Any]:
	"""Normalize BlueCart product response to our unified product fields."""
	return {
		"product_sku": product.get("sku"),
		"full_product_description": product.get("description"),
		"product_images": product.get("images"),
		"in_stock": product.get("in_stock", True),
		"units_available": product.get("units_available"),
	}


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
	"""Normalize BlueCart offer response to our unified offer fields."""
	seller = offer.get("seller") or {}
	return {
		"price": offer.get("price"),
		"currency": offer.get("currency_symbol", "USD"),
		"seller_name": seller.get("name"),
		"seller_id": seller.get("id"),
		"url": seller.get("url"),
	}


def main(args):
	parser = argparse.ArgumentParser(description="Walmart scraper with optimized seller enrichment")
	parser.add_argument("--keywords", required=True, help="Comma-separated keywords to search")
	parser.add_argument("--max-per-keyword", type=int, default=50, help="Max products per keyword")
	parser.add_argument("--max-pages", type=int, default=50, help="Max pages per keyword")
	parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests")
	parser.add_argument("--export", choices=["csv", "json", "both"], default="csv", help="Export format")
	parser.add_argument("--debug", action="store_true", help="Enable debug output")
	parser.add_argument("--zipcode", help="Customer zipcode for localization")
	parser.add_argument("--category-id", help="Category ID filter")
	
	parsed_args = parser.parse_args(args)
	
	# Initialize
	config = get_config()
	client = BlueCartClient(sleep_seconds=parsed_args.sleep)
	init_db()
	
	keywords = [kw.strip() for kw in parsed_args.keywords.split(",")]
	
	print(f"[{_ts()}] Start OPTIMIZED scan | domain={client.site} | keywords={len(keywords)} | max_per_keyword={parsed_args.max_per_keyword} | max_pages={parsed_args.max_pages}")
	
	all_listings = []
	pending_sellers = []
	
	for kw in keywords:
		print(f"[{_ts()}] Keyword: {kw}")
		collected = 0
		page = 1
		
		while collected < parsed_args.max_per_keyword and page <= parsed_args.max_pages:
			print(f"[{_ts()}]  Page {page}")
			
			extra: Dict[str, Any] = {}
			if parsed_args.zipcode:
				extra["zipcode"] = parsed_args.zipcode
			if parsed_args.category_id:
				extra["category_id"] = parsed_args.category_id
			
			search_resp = client.search(kw, page=page, extra=extra)
			
			if parsed_args.debug and page == 1:
				write_debug_json(search_resp, f"debug_search_{kw.replace(' ', '_')}.json")
			
			items = search_resp.get("search_results") or search_resp.get("items") or []
			if not items:
				items = (search_resp.get("results") or [])
			if not items and isinstance(search_resp.get("data"), dict):
				data = search_resp["data"]
				items = data.get("search_results") or data.get("items") or data.get("results") or []
			
			if not items:
				print(f"[{_ts()}]   No items returned; stopping pagination for keyword")
				break
			
			print(f"[{_ts()}]   Found {len(items)} items on page {page}")
			
			for raw in items:
				if collected >= parsed_args.max_per_keyword:
					break
				
				listing = normalize_listing_from_search(raw)
				listing_id = str(listing.get("listing_id")) if listing.get("listing_id") is not None else None
				
				if not listing_id:
					continue
				
				# Skip walmart.ca URLs - they don't support seller enrichment
				listing_url = listing.get("listing_url", "")
				if "walmart.ca" in listing_url:
					if parsed_args.debug:
						print(f"[{_ts()}]   Skipping walmart.ca URL: {listing_id}")
					continue
				
				upsert_listing_summary(listing_id, listing.get("listing_title"), listing.get("brand"), listing.get("listing_url"))
				
				# Get product details
				product_data = {}
				try:
					product_resp = client.product(listing_id)
					if parsed_args.debug:
						write_debug_json(product_resp, f"debug_product_{listing_id}.json")
					product_data = normalize_product(product_resp.get("product") or product_resp)
				except Exception:
					product_data = normalize_product(_safe_get(raw, "product", default={}) or {})
				
				# Get offer details
				primary_offer = _safe_get(raw, "offers", "primary") or {}
				offers = [primary_offer] if primary_offer else []
				normalized_offers = [normalize_offer(o) for o in offers]
				
				# FAST seller enrichment - only try BlueCart API with numeric seller_id
				primary_o = normalized_offers[0] if normalized_offers else {}
				enriched: Dict[str, Any] = {}
				
				# Only try seller enrichment if we have a numeric seller_id
				numeric_sid: Optional[str] = None
				if primary_o.get("seller_id") and _is_numeric_string(str(primary_o.get("seller_id"))):
					numeric_sid = str(primary_o.get("seller_id"))
				else:
					# search the raw search item for a numeric seller id
					numeric_sid = _collect_numeric_seller_id(raw) or _collect_numeric_seller_id(product_resp or {})
				
				if numeric_sid and client.site == "walmart.com":
					try:
						sp = client.seller_profile(seller_id=numeric_sid)
						if parsed_args.debug:
							write_debug_json(sp, f"debug_seller_profile_id_{primary_o.get('seller_id') or 'none'}_{listing_id}.json")
						
						enriched = _extract_seller_fields(sp)
						
						if parsed_args.debug and enriched:
							present_keys = [k for k, v in enriched.items() if v]
							print(f"[{_ts()}]   Seller enriched: {listing_id} | seller_id={numeric_sid} | keys={present_keys}")
						
					except Exception as e:
						if parsed_args.debug:
							print(f"[{_ts()}]   Seller enrichment failed: {listing_id} | seller_id={numeric_sid} | error={e}")
				
				# Combine all data
				combined = {
					"keyword": kw,
					"item_number": listing_id,
					**listing,
					**product_data,
					**enriched,
				}
				
				# Store in database
				insert_listing_snapshot(listing_id, combined)
				if enriched:
					insert_seller_snapshot(listing_id, primary_o.get("seller_id"), enriched)
				
				all_listings.append(combined)
				collected += 1
				
				if collected % 5 == 0:
					print(f"[{_ts()}]   Collected {collected}/{parsed_args.max_per_keyword} for '{kw}'")
			
			page += 1
		
		print(f"[{_ts()}] Keyword done: {kw} | collected={collected}")
	
	print(f"[{_ts()}] Scan complete: {len(all_listings)} total products")
	
	# Export results
	if parsed_args.export in ["csv", "both"]:
		export_csv(all_listings, "walmart_scan")
		print(f"[{_ts()}] CSV exported: output\\walmart_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
	
	if parsed_args.export in ["json", "both"]:
		export_json(all_listings, "walmart_scan")
		print(f"[{_ts()}] JSON exported: output\\walmart_scan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
	
	print(f"[{_ts()}] Results exported")


if __name__ == "__main__":
	import sys
	main(sys.argv[1:])
