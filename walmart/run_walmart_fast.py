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


def normalize_listing_from_search(raw: Dict[str, Any]) -> Dict[str, Any]:
	"""Extract listing fields from BlueCart search result."""
	product = raw.get("product") or {}
	offers = raw.get("offers") or {}
	primary_offer = offers.get("primary") or {}
	
	return {
		"listing_id": product.get("item_id") or raw.get("item_id") or raw.get("id"),
		"listing_title": product.get("title") or raw.get("title"),
		"listing_url": product.get("link") or product.get("url") or raw.get("url"),
		"brand": product.get("brand") or raw.get("brand"),
		"product_sku": product.get("sku") or raw.get("sku"),
		"full_product_description": product.get("description") or raw.get("description"),
		"product_images": product.get("images") or raw.get("images"),
		"price": primary_offer.get("price") or raw.get("price"),
		"currency": primary_offer.get("currency") or raw.get("currency"),
		"in_stock": primary_offer.get("in_stock") or raw.get("in_stock"),
		"units_available": primary_offer.get("units_available") or raw.get("units_available"),
		"offers_count": raw.get("offers_count") or len(raw.get("offers", {}).get("all", [])),
		"seller_name": primary_offer.get("seller_name") or raw.get("seller_name"),
		"seller_id": primary_offer.get("seller_id") or raw.get("seller_id"),
		"seller_rating": primary_offer.get("seller_rating") or raw.get("seller_rating"),
		"seller_profile_url": primary_offer.get("seller_url") or raw.get("seller_url"),
		"total_reviews": product.get("reviews_count") or raw.get("reviews_count"),
		"keyword": raw.get("keyword"),
		"country": raw.get("country"),
		"state_province": raw.get("state_province"),
		"zip_code": raw.get("zip_code"),
	}


def normalize_product(product: Dict[str, Any]) -> Dict[str, Any]:
	"""Extract product fields from BlueCart product result."""
	return {
		"product_sku": product.get("sku"),
		"full_product_description": product.get("description"),
		"product_images": product.get("images"),
		"brand": product.get("brand"),
	}


def normalize_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
	"""Extract offer fields from BlueCart offer result."""
	return {
		"seller_name": offer.get("seller_name"),
		"seller_id": offer.get("seller_id"),
		"seller_rating": offer.get("seller_rating"),
		"seller_profile_url": offer.get("seller_url") or offer.get("url"),
		"price": offer.get("price"),
		"currency": offer.get("currency"),
		"in_stock": offer.get("in_stock"),
		"units_available": offer.get("units_available"),
	}


def main():
	parser = argparse.ArgumentParser(description="Fast Walmart scraper for timing tests")
	parser.add_argument("--keywords", required=True, help="Comma-separated keywords")
	parser.add_argument("--max-per-keyword", type=int, default=10, help="Max products per keyword")
	parser.add_argument("--max-pages", type=int, default=50, help="Max pages per keyword")
	parser.add_argument("--export", choices=["json", "csv"], default="csv", help="Export format")
	parser.add_argument("--debug", action="store_true", help="Enable debug output")
	args = parser.parse_args()
	
	config = get_config()
	client = BlueCartClient(config.api_key, config.base_url, config.site)
	
	init_db()
	
	keywords = [kw.strip() for kw in args.keywords.split(",")]
	total_collected = 0
	all_listings = []
	
	print(f"[{_ts()}] Start FAST scan | domain={config.site} | keywords={len(keywords)} | max_per_keyword={args.max_per_keyword} | max_pages={args.max_pages}")
	
	for kw in keywords:
		print(f"[{_ts()}] Keyword: {kw}")
		collected = 0
		page = 1
		
		while collected < args.max_per_keyword and page <= args.max_pages:
			print(f"[{_ts()}]  Page {page}")
			
			try:
				search_resp = client.search(kw, page=page)
				if args.debug and page == 1:
					write_debug_json(search_resp, f"debug_search_{kw.replace(' ', '_')}.json")
				
				items = search_resp.get("search_results") or search_resp.get("items") or []
				if not items:
					items = (search_resp.get("results") or [])
				if not items and isinstance(search_resp.get("data"), dict):
					data = search_resp["data"]
					items = data.get("search_results") or data.get("items") or data.get("results") or []
				
				print(f"[{_ts()}]   Found {len(items)} items on page {page}")
				
				if not items:
					print(f"[{_ts()}]   No items returned; stopping pagination for keyword")
					break
				
				for raw in items:
					if collected >= args.max_per_keyword:
						break
					
					listing = normalize_listing_from_search(raw)
					listing_id = str(listing.get("listing_id")) if listing.get("listing_id") is not None else None
					if not listing_id:
						continue
					
					# Skip heavy seller enrichment - just use basic data
					listing["keyword"] = kw
					
					# Store basic data
					upsert_listing_summary(listing_id, listing.get("listing_title"), listing.get("brand"), listing.get("listing_url"))
					insert_listing_snapshot(listing_id, listing)
					
					all_listings.append(listing)
					collected += 1
					total_collected += 1
					
					if collected % 5 == 0:
						print(f"[{_ts()}]   Collected {collected}/{args.max_per_keyword} for '{kw}'")
				
				page += 1
				
			except Exception as e:
				print(f"[{_ts()}]   Error on page {page}: {e}")
				break
		
		print(f"[{_ts()}] Completed '{kw}': {collected} products")
	
	print(f"[{_ts()}] Scan complete: {total_collected} total products")
	
	# Export results
	if args.export == "csv":
		export_csv(all_listings, "walmart_fast_scan")
	elif args.export == "json":
		export_json(all_listings, "walmart_fast_scan")
	
	print(f"[{_ts()}] Results exported")


if __name__ == "__main__":
	main()
