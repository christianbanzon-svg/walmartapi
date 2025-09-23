import argparse
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bluecart_client import BlueCartClient
from config import get_config
from storage import init_db, insert_listing_snapshot, insert_seller_snapshot, upsert_listing_summary
from exporters import export_json, export_csv, write_debug_json
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
	}


def run(keyword_list: List[str], max_per_keyword: int, export: List[str], sleep: float, offers_export: bool, max_pages: int, debug: bool, walmart_domain: Optional[str] = None, category_id: Optional[str] = None, retry_seller_passes: int = 0, retry_seller_delay: float = 15.0) -> None:
	init_db()
	cfg = get_config()
	# Use custom domain if provided, otherwise use default from config
	domain_to_use = walmart_domain or cfg.site
	client = BlueCartClient(sleep_seconds=sleep, site=domain_to_use)
	print(f"[{_ts()}] Start scan | domain={client.site} | keywords={len(keyword_list)} | max_per_keyword={max_per_keyword} | max_pages={max_pages}")

	all_records: List[Dict[str, Any]] = []
	all_offers: List[Dict[str, Any]] = []
	# Cache and pending lists for seller retries
	seller_cache: Dict[str, Dict[str, Any]] = {}
	pending_sellers: List[Tuple[Optional[str], Optional[str]]] = []

	for kw in keyword_list:
		print(f"[{_ts()}] Keyword: {kw}")
		collected = 0
		page = 1
		while collected < max_per_keyword and page <= max_pages:
			print(f"[{_ts()}]  Page {page}")
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
				print(f"[{_ts()}]   No items returned; stopping pagination for keyword")
				break
			for raw in items:
				if collected >= max_per_keyword:
					break
				listing = normalize_listing_from_search(raw)
				listing_id = str(listing.get("listing_id")) if listing.get("listing_id") is not None else None
				if not listing_id:
					continue
				upsert_listing_summary(listing_id, listing.get("listing_title"), listing.get("brand"), listing.get("url"))
				# Try API product details first for SKU/full description; fallback to search payload
				product_data = {}
				try:
					product_resp = client.product(listing_id)
					if debug:
						write_debug_json(product_resp, f"debug_product_{listing_id}.json")
					product_data = normalize_product(product_resp.get("product") or product_resp)
				except Exception:
					product_data = normalize_product(_safe_get(raw, "product", default={}) or {})
				primary_offer = _safe_get(raw, "offers", "primary") or {}
				offers = [primary_offer] if primary_offer else []
				normalized_offers = [normalize_offer(o) for o in offers]
				# Derive primary seller details and try enrichment via BlueCart (US only), else product page scrape
				primary_o = normalized_offers[0] if normalized_offers else {}
				enriched: Dict[str, Any] = {}
				# Try BlueCart seller_profile using numeric seller_id if present anywhere in raw/product payloads
				numeric_sid: Optional[str] = None
				if primary_o.get("seller_id") and _is_numeric_string(str(primary_o.get("seller_id"))):
					numeric_sid = str(primary_o.get("seller_id"))
				else:
					# search the raw search item for a numeric seller id
					numeric_sid = _collect_numeric_seller_id(raw) or _collect_numeric_seller_id(product_resp or {})
				if client.site == "walmart.com" and (numeric_sid or primary_o.get("url")):
					try:
						sp = client.seller_profile(seller_id=numeric_sid, url=None if numeric_sid else primary_o.get("url"))
						if debug:
							write_debug_json(sp, f"debug_seller_profile_id_{primary_o.get('seller_id') or 'none'}_{listing_id}.json")
							# Print diagnostics if BlueCart returns request_info
							ri = sp.get("request_info") if isinstance(sp, dict) else None
							if ri:
								msg = ri.get("message") if isinstance(ri, dict) else None
								print(f"[{_ts()}]   seller_profile diag: seller_id={numeric_sid or primary_o.get('seller_id')} message={msg}")
						# Extract unified fields from response
						enriched = _extract_seller_fields(sp)
					except Exception:
						pass
				if not enriched:
					# Simplified: skip seller profile enrichment for now
					enriched = {}
					# Note: seller_profile_url was removed during cleanup, skip this section for now
					pass
				# If still missing contact info, attempt web enrichment by seller name
				if (not enriched.get("email_address") or not enriched.get("phone_number")) and primary_o.get("seller_name"):
					try:
						# Simplified: skip web enrichment for now
						we = {}
						if we:
							for k in ("email_address", "phone_number", "address", "country", "state_province", "zip_code"):
								if not enriched.get(k) and we.get(k):
									enriched[k] = we[k]
					except Exception:
						pass
				# Preserve the primary enriched dict for the combined listing output
				enriched_primary = dict(enriched) if enriched else {}
				# Track pending sellers for a later retry pass if still missing contact fields
				sid_for_retry = primary_o.get("seller_id")
				url_for_retry = enriched_primary.get("seller_profile_url") or primary_o.get("url")
				if (not enriched_primary.get("email_address") and not enriched_primary.get("phone_number")) and (sid_for_retry or url_for_retry):
					pending_sellers.append((sid_for_retry, url_for_retry))
				# Optional debug for each collected item
				if debug:
					present_keys = [k for k, v in enriched_primary.items() if v]
					print(f"[{_ts()}]   Seller debug listing_id={listing_id} seller_id={primary_o.get('seller_id')} name={primary_o.get('seller_name')} url={primary_o.get('url')} enriched_keys={present_keys}")
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
				# Simplified: skip availability probe for now
				avail = {}
				combined = {
					"keyword": kw,
					"listing_id": listing_id,
					"listing_title": listing.get("title"),
					"product_images": images_joined,
					"product_sku": product_data.get("sku"),
						"item_number": listing_id,
						"price": listing.get("price"),
						"currency": listing.get("currency"),
						"units_available": avail.get("units_available"),
						"in_stock": in_stock,
						"brand": listing.get("brand") or product_data.get("brand"),
						"listing_url": listing.get("url"),
						"full_product_description": product_data.get("description"),
						# Seller fields (from primary offer + enrichment)
						"seller_name": primary_o.get("seller_name"),
						"seller_email": enriched_primary.get("email_address"),
						"seller_profile_picture": enriched_primary.get("seller_profile_picture"),
						"seller_profile_url": enriched_primary.get("seller_profile_url") or primary_o.get("url"),
						"seller_rating": enriched_primary.get("seller_rating") or primary_o.get("seller_rating"),
						"total_reviews": enriched_primary.get("total_reviews") or primary_o.get("total_reviews"),
						"email_address": enriched_primary.get("email_address"),
						"business_legal_name": enriched_primary.get("business_legal_name"),
						"country": enriched_primary.get("country"),
						"state_province": enriched_primary.get("state_province"),
						"zip_code": enriched_primary.get("zip_code"),
						"phone_number": enriched_primary.get("phone_number"),
						"address": enriched_primary.get("address"),
						"offers_count": len(normalized_offers),
						# internal keys to assist retry reconciliation
						"_primary_seller_id": sid_for_retry,
						"_primary_seller_url": url_for_retry,
				}
				all_records.append(combined)
				collected += 1
				if collected % 5 == 0 or collected == 1:
					print(f"[{_ts()}]   Collected {collected}/{max_per_keyword} for '{kw}'")
			page += 1
		print(f"[{_ts()}] Keyword done: {kw} | collected={collected}")

	# Retry pass for seller_profile if requested (US only)
	if retry_seller_passes > 0 and client.site == "walmart.com" and pending_sellers:
		# unique pending set
		unique_pending: List[Tuple[Optional[str], Optional[str]]] = []
		seen_keys: set = set()
		for sid, surl in pending_sellers:
			key = f"sid:{sid}" if sid else f"url:{surl}"
			if key in seen_keys:
				continue
			seen_keys.add(key)
			unique_pending.append((sid, surl))
		for attempt in range(retry_seller_passes):
			if debug:
				print(f"[{_ts()}] Retry seller_profile pass {attempt+1}/{retry_seller_passes} | pending={len(unique_pending)}")
			still_pending: List[Tuple[Optional[str], Optional[str]]] = []
			for sid, surl in unique_pending:
				key = f"sid:{sid}" if sid else f"url:{surl}"
				if key in seller_cache:
					continue
				try:
					sp = client.seller_profile(seller_id=sid, url=surl)
					fields = _extract_seller_fields(sp)
					if fields.get("email_address") or fields.get("phone_number"):
						seller_cache[key] = fields
					else:
						still_pending.append((sid, surl))
				except Exception:
					still_pending.append((sid, surl))
				# pace requests across the delay window
				time.sleep(max(0.05, retry_seller_delay / max(1, len(unique_pending))))
			unique_pending = still_pending
			if not unique_pending:
				break
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

	# export
	name_prefix = "walmart_scan"
	# Clean up records: drop empties and dedupe
	seen: set = set()
	cleaned_records: List[Dict[str, Any]] = []
	for r in all_records:
		key = (r.get("listing_id"), r.get("keyword"))
		if not r.get("listing_id") or not r.get("listing_title"):
			continue
		if key in seen:
			continue
		seen.add(key)
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

	if "json" in export:
		json_path = export_json(cleaned_records, name_prefix)
		print(f"[{_ts()}] JSON exported: {json_path}")
	if "csv" in export:
		csv_path = export_csv(cleaned_records, name_prefix)
		print(f"[{_ts()}] CSV exported: {csv_path}")
	if offers_export and cleaned_offers:
		if "json" in export:
			o_json_path = export_json(cleaned_offers, name_prefix + "_offers")
			print(f"[{_ts()}] Offers JSON exported: {o_json_path}")
		if "csv" in export:
			o_csv_path = export_csv(cleaned_offers, name_prefix + "_offers")
			print(f"[{_ts()}] Offers CSV exported: {o_csv_path}")


def main(args=None):
	parser = argparse.ArgumentParser(description="Walmart scraper via BlueCart API")
	parser.add_argument("--keywords", type=str, default="", help="Comma-separated keywords")
	parser.add_argument("--keywords-file", type=str, default="", help="Path to file with one keyword per line")
	parser.add_argument("--max-per-keyword", type=int, default=10)
	parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between API calls")
	parser.add_argument("--export", nargs="+", default=["json", "csv"], choices=["json", "csv"])
	parser.add_argument("--offers-export", action="store_true", help="Export per-offer dataset in addition to listings")
	parser.add_argument("--max-pages", type=int, default=50, help="Max pages to paginate for search")
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


