#!/usr/bin/env python3
"""
Walmart ID Crawler - Fast Simple Version
Optimized for speed with minimal complexity
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
import argparse
import os

from bluecart_client import BlueCartClient
from config import get_config
from storage import insert_listing_snapshot, insert_seller_snapshot
from exporters import export_csv, export_json

# Global seller cache for performance
SELLER_CACHE = {}
CACHE_HITS = 0
CACHE_MISSES = 0

def _safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary."""
    try:
        return data.get(key, default)
    except (KeyError, TypeError):
        return default

def _is_numeric_string(value: Any) -> bool:
    """Check if a value is a numeric string."""
    if not isinstance(value, str):
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False

def normalize_listing_from_product(product_data: Dict[str, Any], item_id: str) -> Dict[str, Any]:
    """Normalize product data from BlueCart product API response."""
    return {
        "listing_id": item_id,
        "listing_title": _safe_get(product_data, "title", default=""),
        "brand": _safe_get(product_data, "brand", default=""),
        "listing_url": _safe_get(product_data, "link", default=f"https://www.walmart.com/ip/{item_id}"),
        "price": _safe_get(product_data, "price", default=""),
        "currency": _safe_get(product_data, "currency", default="USD"),
        "in_stock": _safe_get(product_data, "in_stock", default=True),
        "product_sku": _safe_get(product_data, "model", default=""),
        "full_product_description": _safe_get(product_data, "description_full", default=_safe_get(product_data, "description", default="")),
        "product_images": json.dumps(_safe_get(product_data, "images", default=[])),
        "total_reviews": _safe_get(product_data, "ratings_total", default=0),
        "rating": _safe_get(product_data, "rating", default=0),
        "item_number": _safe_get(product_data, "item_number", default=""),
        "upc": _safe_get(product_data, "upc", default=""),
        "product_id": _safe_get(product_data, "product_id", default=""),
    }

def _collect_numeric_seller_id(product_data: Dict[str, Any]) -> Optional[str]:
    """Try to find a numeric seller ID in product data."""
    possible_keys = [
        "seller_id", "sellerId", "seller", "vendor_id", "vendorId", 
        "merchant_id", "merchantId", "store_id", "storeId"
    ]
    
    for key in possible_keys:
        value = _safe_get(product_data, key)
        if value and _is_numeric_string(value):
            return str(value)
    
    return None

async def enrich_seller_data_cached(
    session: aiohttp.ClientSession,
    client: BlueCartClient,
    seller_id: str,
    seller_url: str,
    item_id: str
) -> Dict[str, Any]:
    """Enrich seller data with caching."""
    global SELLER_CACHE, CACHE_HITS, CACHE_MISSES
    
    cache_key = f"{seller_id}_{seller_url}"
    if cache_key in SELLER_CACHE:
        CACHE_HITS += 1
        print(f"🎯 Cache HIT for seller {seller_id}")
        return SELLER_CACHE[cache_key]
    
    CACHE_MISSES += 1
    print(f"🔍 Cache MISS for seller {seller_id} - fetching...")
    
    try:
        seller_profile = client.seller_profile(seller_id, seller_url)
        
        enriched_seller = {
            "seller_id": seller_id,
            "seller_name": _safe_get(seller_profile, "name", default=""),
            "seller_profile_url": seller_url,
            "seller_profile_picture": _safe_get(seller_profile, "profile_picture", default=""),
            "seller_rating": _safe_get(seller_profile, "rating", default=0),
            "total_reviews_seller": _safe_get(seller_profile, "total_reviews", default=0),
            "email_address": _safe_get(seller_profile, "email", default=""),
            "phone_number": _safe_get(seller_profile, "phone", default=""),
            "address": _safe_get(seller_profile, "address", default=""),
            "business_legal_name": _safe_get(seller_profile, "business_name", default=""),
            "country": _safe_get(seller_profile, "country", default=""),
            "state_province": _safe_get(seller_profile, "state", default=""),
            "zip_code": _safe_get(seller_profile, "zip", default=""),
            "data_source": "seller_profile_api",
            "enrichment_status": "enriched"
        }
        
        SELLER_CACHE[cache_key] = enriched_seller
        print(f"✅ Seller {seller_id} enriched and cached")
        return enriched_seller
        
    except Exception as e:
        print(f"⚠️  Failed to enrich seller {seller_id}: {e}")
        fallback_data = {
            "seller_id": seller_id,
            "seller_name": "Walmart",
            "seller_profile_url": seller_url,
            "seller_profile_picture": "",
            "seller_rating": 0,
            "total_reviews_seller": 0,
            "email_address": "",
            "phone_number": "",
            "address": "",
            "business_legal_name": "Walmart Inc.",
            "country": "US",
            "state_province": "",
            "zip_code": "",
            "data_source": "fallback",
            "enrichment_status": "basic"
        }
        
        SELLER_CACHE[cache_key] = fallback_data
        return fallback_data

async def process_item_id_fast(
    session: aiohttp.ClientSession,
    client: BlueCartClient,
    item_id: str,
    skip_seller_enrichment: bool = False,
    sleep: float = 0.05
) -> Optional[Dict[str, Any]]:
    """Process a single item ID with optimized performance."""
    try:
        print(f"🔄 Processing item ID: {item_id}")
        
        # Get product data
        product_response = client.product(item_id)
        if not product_response:
            print(f"❌ No product data for {item_id}")
            return None
        
        # Normalize listing data
        listing = normalize_listing_from_product(product_response, item_id)
        
        # Get offers data (with fallback)
        offers_data = []
        try:
            offers_response = client.offers(item_id)
            offers_data = _safe_get(offers_response, "offers", default=[])
        except Exception as e:
            print(f"⚠️  Offers API not supported or failed: {e}")
            offers_data = _safe_get(product_response, "offers", default=[])
        
        # Extract seller information
        seller_id = None
        seller_url = None
        
        if offers_data:
            first_offer = offers_data[0]
            seller_id = _safe_get(first_offer, "seller_id")
            seller_url = _safe_get(first_offer, "seller_url")
        
        # Enrich seller data (unless skipped)
        if skip_seller_enrichment:
            enriched_seller = {
                "seller_id": "walmart",
                "seller_name": "Walmart",
                "seller_profile_url": seller_url or "",
                "seller_profile_picture": "",
                "seller_rating": 0,
                "total_reviews_seller": 0,
                "email_address": "",
                "phone_number": "",
                "address": "",
                "business_legal_name": "Walmart Inc.",
                "country": "US",
                "state_province": "",
                "zip_code": "",
                "data_source": "skipped",
                "enrichment_status": "skipped"
            }
        elif seller_id and _is_numeric_string(seller_id):
            enriched_seller = await enrich_seller_data_cached(
                session, client, seller_id, seller_url, item_id
            )
        else:
            # Try to find seller ID in product data
            numeric_seller_id = _collect_numeric_seller_id(product_response)
            if numeric_seller_id:
                enriched_seller = await enrich_seller_data_cached(
                    session, client, numeric_seller_id, seller_url, item_id
                )
            else:
                enriched_seller = {
                    "seller_id": "walmart",
                    "seller_name": "Walmart",
                    "seller_profile_url": seller_url or "",
                    "seller_profile_picture": "",
                    "seller_rating": 0,
                    "total_reviews_seller": 0,
                    "email_address": "",
                    "phone_number": "",
                    "address": "",
                    "business_legal_name": "Walmart Inc.",
                    "country": "US",
                    "state_province": "",
                    "zip_code": "",
                    "data_source": "product_data",
                    "enrichment_status": "basic"
                }
        
        # Combine data
        combined_data = {**listing, **enriched_seller}
        combined_data["offers_count"] = len(offers_data)
        
        print(f"✅ Item {item_id} processed successfully")
        
        # Small delay
        if sleep > 0:
            await asyncio.sleep(sleep)
        
        return combined_data
        
    except Exception as e:
        print(f"❌ Error processing item {item_id}: {e}")
        return None

async def run_fast_id_crawler(
    item_ids: List[str],
    export_formats: List[str] = ["csv"],
    debug: bool = False,
    sleep: float = 0.05,
    skip_seller_enrichment: bool = False,
    max_concurrent: int = 10
) -> List[Dict[str, Any]]:
    """Run the fast ID crawler with optimized performance."""
    config = get_config()
    
    # Create optimized HTTP session
    connector = aiohttp.TCPConnector(
        limit=20,
        limit_per_host=10,
        keepalive_timeout=30,
        enable_cleanup_closed=True
    )
    
    timeout = aiohttp.ClientTimeout(
        total=30,
        connect=10,
        sock_read=10
    )
    
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    ) as session:
        client = BlueCartClient(config.api_key, config.base_url)
        
        print(f"🚀 Starting FAST ID crawler for {len(item_ids)} items")
        print(f"⚡ Skip seller enrichment: {skip_seller_enrichment}")
        print(f"🔄 Max concurrent: {max_concurrent}")
        print(f"⏱️  Sleep: {sleep}s")
        
        start_time = time.time()
        
        # Process items concurrently
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_semaphore(item_id: str):
            async with semaphore:
                return await process_item_id_fast(
                    session, client, item_id, skip_seller_enrichment, sleep
                )
        
        tasks = [process_with_semaphore(item_id) for item_id in item_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter valid results
        valid_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"\n🎉 FAST crawler completed!")
        print(f"⏱️  Total time: {total_time:.2f} seconds")
        print(f"📊 Items processed: {len(valid_results)}")
        print(f"⚡ Average time per item: {total_time/len(valid_results):.2f} seconds")
        print(f"🎯 Cache hits: {CACHE_HITS}")
        print(f"🔍 Cache misses: {CACHE_MISSES}")
        if CACHE_HITS + CACHE_MISSES > 0:
            print(f"📈 Cache hit rate: {CACHE_HITS/(CACHE_HITS+CACHE_MISSES)*100:.1f}%")
        
        # Store results in database
        for result in valid_results:
            try:
                insert_listing_snapshot(result["listing_id"], result)
                insert_seller_snapshot(result["listing_id"], result["seller_id"], result)
            except Exception as e:
                print(f"⚠️  Database error: {e}")
        
        # Export results
        if valid_results:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for export_format in export_formats:
                if export_format == "csv":
                    csv_file = export_csv(
                        valid_results,
                        name_prefix=f"walmart_id_crawl_fast_{timestamp}"
                    )
                    print(f"📄 CSV exported: {csv_file}")
                
                elif export_format == "json":
                    json_file = export_json(
                        valid_results,
                        name_prefix=f"walmart_id_crawl_fast_{timestamp}"
                    )
                    print(f"📄 JSON exported: {json_file}")
        
        return valid_results

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Fast Walmart ID Crawler")
    parser.add_argument("--item-ids", required=True, help="Comma-separated list of item IDs")
    parser.add_argument("--export", default="csv", help="Export format: csv, json, or both")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--sleep", type=float, default=0.05, help="Sleep between requests (seconds)")
    parser.add_argument("--skip-seller-enrichment", action="store_true", help="Skip seller enrichment for speed")
    parser.add_argument("--max-concurrent", type=int, default=10, help="Maximum concurrent requests")
    
    args = parser.parse_args()
    
    # Parse item IDs
    item_ids = [id.strip() for id in args.item_ids.split(",") if id.strip()]
    if not item_ids:
        print("❌ No valid item IDs provided")
        return
    
    # Parse export formats
    if args.export == "both":
        export_formats = ["csv", "json"]
    else:
        export_formats = [args.export]
    
    print(f"🚀 Starting FAST Walmart ID crawler")
    print(f"📋 Item IDs: {item_ids}")
    print(f"📤 Export formats: {export_formats}")
    print(f"⚡ Skip seller enrichment: {args.skip_seller_enrichment}")
    print(f"🔄 Max concurrent: {args.max_concurrent}")
    print(f"⏱️  Sleep: {args.sleep}s")
    
    # Run the crawler
    try:
        results = asyncio.run(run_fast_id_crawler(
            item_ids=item_ids,
            export_formats=export_formats,
            debug=args.debug,
            sleep=args.sleep,
            skip_seller_enrichment=args.skip_seller_enrichment,
            max_concurrent=args.max_concurrent
        ))
        
        print(f"\n✅ FAST crawler completed successfully!")
        print(f"📊 Total results: {len(results)}")
        
    except KeyboardInterrupt:
        print("\n⏹️  Crawler interrupted by user")
    except Exception as e:
        print(f"\n❌ Crawler failed: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
