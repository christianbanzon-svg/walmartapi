import argparse
import time
import asyncio
import aiohttp
import concurrent.futures
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


async def enrich_seller_parallel(session: aiohttp.ClientSession, api_key: str, seller_id: str, listing_id: str, debug: bool = False) -> Tuple[str, Dict[str, Any]]:
	"""Enrich a single seller using async HTTP requests."""
	try:
		params = {
			"api_key": api_key,
			"source": "walmart",
			"walmart_domain": "walmart.com",
			"type": "seller_profile",
			"seller_id": seller_id
		}
		
		async with session.get("https://api.bluecartapi.com/request", params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
			if response.status == 200:
				data = await response.json()
				if debug:
					write_debug_json(data, f"debug_seller_profile_id_{seller_id}_{listing_id}.json")
				
				enriched = _extract_seller_fields(data)
				return listing_id, enriched
			else:
				return listing_id, {}
	except Exception as e:
		if debug:
			print(f"[{_ts()}]   Seller enrichment failed: {listing_id} | seller_id={seller_id} | error={e}")
		return listing_id, {}


def main(args):
	parser = argparse.ArgumentParser(description="Walmart scraper with ULTRA-FAST parallel seller enrichment")
	parser.add_argument("--keywords", required=True, help="Comma-separated keywords to search")
	parser.add_argument("--max-per-keyword", type=int, default=50, help="Max products per keyword")
	parser.add_argument("--max-pages", type=int, default=50, help="Max pages per keyword")
	parser.add_argument("--sleep", type=float, default=1.0, help="Sleep between requests")
	parser.add_argument("--export", choices=["csv", "json", "both"], default="csv", help="Export format")
	parser.add_argument("--debug", action="store_true", help="Enable debug output")
	parser.add_argument("--zipcode", help="Customer zipcode for localization")
	parser.add_argument("--category-id", help="Category ID filter")
	parser.add_argument("--max-workers", type=int, default=10, help="Max parallel workers for seller enrichment")
	
	parsed_args = parser.parse_args(args)
	
	# Initialize
	config = get_config()
	client = BlueCartClient(sleep_seconds=parsed_args.sleep)
	init_db()
	
	keywords = [kw.strip() for kw in parsed_args.keywords.split(",")]
	
	print(f"[{_ts()}] Start ULTRA-FAST scan | domain={client.site} | keywords={len(keywords)} | max_per_keyword={parsed_args.max_per_keyword} | max_workers={parsed_args.max_workers}")
	
	all_listings = []
	
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
			
			# Process items in batches for parallel enrichment
			batch_items = []
			seller_enrichment_tasks = []
			
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
				
				# Prepare for parallel seller enrichment
				primary_o = normalized_offers[0] if normalized_offers else {}
				numeric_sid: Optional[str] = None
				if primary_o.get("seller_id") and _is_numeric_string(str(primary_o.get("seller_id"))):
					numeric_sid = str(primary_o.get("seller_id"))
				else:
					numeric_sid = _collect_numeric_seller_id(raw) or _collect_numeric_seller_id(product_resp or {})
				
				# Combine basic data
				combined = {
					"keyword": kw,
					"item_number": listing_id,
					**listing,
					**product_data,
				}
				
				batch_items.append((listing_id, combined, numeric_sid, primary_o.get("seller_id")))
				collected += 1
				
				if collected % 5 == 0:
					print(f"[{_ts()}]   Collected {collected}/{parsed_args.max_per_keyword} for '{kw}'")
			
			# PARALLEL SELLER ENRICHMENT
			if batch_items:
				print(f"[{_ts()}]   Starting parallel seller enrichment for {len(batch_items)} items...")
				start_time = time.time()
				
				# Use asyncio for parallel HTTP requests
				async def enrich_all_sellers():
					async with aiohttp.ClientSession() as session:
						tasks = []
						for listing_id, combined, numeric_sid, seller_id in batch_items:
							if numeric_sid and client.site == "walmart.com":
								task = enrich_seller_parallel(session, config.api_key, numeric_sid, listing_id, parsed_args.debug)
								tasks.append(task)
							else:
								# No seller enrichment needed
								tasks.append(asyncio.create_task(asyncio.sleep(0)))
						
						# Run all tasks in parallel with semaphore to limit concurrency
						semaphore = asyncio.Semaphore(parsed_args.max_workers)
						async def limited_task(task):
							async with semaphore:
								return await task
						
						limited_tasks = [limited_task(task) for task in tasks]
						results = await asyncio.gather(*limited_tasks, return_exceptions=True)
						
						# Process results
						enriched_data = {}
						for i, result in enumerate(results):
							if isinstance(result, tuple):
								listing_id, enriched = result
								enriched_data[listing_id] = enriched
						
						return enriched_data
				
				# Run the async enrichment
				enriched_data = asyncio.run(enrich_all_sellers())
				
				enrichment_time = time.time() - start_time
				print(f"[{_ts()}]   Parallel enrichment completed in {enrichment_time:.2f} seconds")
				
				# Combine enriched data with listings
				for listing_id, combined, numeric_sid, seller_id in batch_items:
					if listing_id in enriched_data:
						combined.update(enriched_data[listing_id])
						if parsed_args.debug and enriched_data[listing_id]:
							present_keys = [k for k, v in enriched_data[listing_id].items() if v]
							print(f"[{_ts()}]   Seller enriched: {listing_id} | seller_id={numeric_sid} | keys={present_keys}")
					
					# Store in database
					insert_listing_snapshot(listing_id, combined)
					if listing_id in enriched_data and enriched_data[listing_id]:
						insert_seller_snapshot(listing_id, seller_id, enriched_data[listing_id])
					
					all_listings.append(combined)
			
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

