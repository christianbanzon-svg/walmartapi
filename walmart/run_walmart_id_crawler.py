#!/usr/bin/env python3
"""
Walmart ID Crawler - Direct Item ID Scraping

This script crawls specific Walmart item IDs to extract the same rich data
as the keyword scraper, including seller information and contact details.

Usage:
    python run_walmart_id_crawler.py --item-ids "5245210374,1234567890" --export csv json
    python run_walmart_id_crawler.py --item-ids-file item_ids.txt --export csv
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bluecart_client import BlueCartClient
from config import get_config
from exporters import export_csv, export_json, ensure_output_dir
from storage import init_db, upsert_listing_summary, insert_listing_snapshot, insert_seller_snapshot


def _safe_get(data: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely get nested dictionary values."""
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


def _is_numeric_string(s: str) -> bool:
    """Check if string is numeric."""
    try:
        float(s)
        return True
    except ValueError:
        return False


def _collect_numeric_seller_id(data: Dict[str, Any]) -> Optional[str]:
    """Extract numeric seller ID from various data structures."""
    # Check common locations for seller IDs
    locations = [
        ["offers", "primary", "seller_id"],
        ["offers", "primary", "seller", "id"],
        ["seller_id"],
        ["seller", "id"],
        ["primary_offer", "seller_id"],
    ]
    
    for location in locations:
        value = _safe_get(data, *location)
        if value and _is_numeric_string(str(value)):
            return str(value)
    
    return None


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


def normalize_offer(offer_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize offer data from BlueCart API response."""
    return {
        "seller_id": _safe_get(offer_data, "seller_id", default=""),
        "seller_name": _safe_get(offer_data, "seller_name", default=""),
        "seller_profile_url": _safe_get(offer_data, "seller_url", default=""),
        "price": _safe_get(offer_data, "price", default=""),
        "currency": _safe_get(offer_data, "currency", default="USD"),
        "shipping": _safe_get(offer_data, "shipping", default=""),
        "availability": _safe_get(offer_data, "availability", default=""),
    }


async def enrich_seller_data(session: aiohttp.ClientSession, client: BlueCartClient, 
                           seller_id: str, seller_url: str, listing_id: str) -> Dict[str, Any]:
    """Enrich seller data using BlueCart seller profile API."""
    enriched = {
        "seller_id": seller_id,
        "seller_name": "",
        "seller_profile_url": seller_url,
        "business_legal_name": "",
        "email_address": "",
        "phone_number": "",
        "address": "",
        "country": "",
        "state_province": "",
        "zip_code": "",
        "seller_rating": 0,
        "seller_profile_picture": "",
        "total_reviews_seller": 0,
        "data_source": "bluecart_api",
        "enrichment_status": "success"
    }
    
    try:
        # Try BlueCart seller profile API
        seller_data = client.seller_profile(seller_id=seller_id, url=seller_url)
        
        if seller_data and "seller" in seller_data:
            seller = seller_data["seller"]
            enriched.update({
                "seller_name": _safe_get(seller, "name", default=""),
                "business_legal_name": _safe_get(seller, "business_name", default=""),
                "email_address": _safe_get(seller, "email", default=""),
                "phone_number": _safe_get(seller, "phone", default=""),
                "address": _safe_get(seller, "address", default=""),
                "country": _safe_get(seller, "country", default=""),
                "state_province": _safe_get(seller, "state", default=""),
                "zip_code": _safe_get(seller, "zip", default=""),
                "seller_rating": _safe_get(seller, "rating", default=0),
                "seller_profile_picture": _safe_get(seller, "profile_picture", default=""),
                "total_reviews_seller": _safe_get(seller, "total_reviews", default=0),
            })
            
            # Store seller snapshot
            insert_seller_snapshot(listing_id, seller_id, seller_data)
            
    except Exception as e:
        print(f"⚠️  Seller enrichment failed for {seller_id}: {e}")
        enriched["enrichment_status"] = "failed"
        enriched["data_source"] = "fallback"
    
    return enriched


async def process_item_id(session: aiohttp.ClientSession, client: BlueCartClient, 
                         item_id: str, debug: bool = False) -> Optional[Dict[str, Any]]:
    """Process a single Walmart item ID and extract all available data."""
    print(f"🔍 Processing item ID: {item_id}")
    
    try:
        # Get product details
        product_response = client.product(item_id)
        
        if debug:
            debug_file = f"debug_product_{item_id}.json"
            with open(debug_file, 'w') as f:
                json.dump(product_response, f, indent=2)
            print(f"📄 Debug data saved to: {debug_file}")
        
        product_data = _safe_get(product_response, "product", default={})
        if not product_data:
            print(f"❌ No product data found for item ID: {item_id}")
            return None
        
        # Normalize listing data
        listing = normalize_listing_from_product(product_data, item_id)
        
        # Store listing summary and snapshot
        upsert_listing_summary(
            listing["listing_id"],
            listing["listing_title"],
            listing["brand"],
            listing["listing_url"]
        )
        insert_listing_snapshot(listing["listing_id"], product_response)
        
        # Get offers data (if supported)
        offers_data = []
        try:
            offers_response = client.offers(item_id)
            offers_data = _safe_get(offers_response, "offers", default=[])
            
            if debug:
                debug_file = f"debug_offers_{item_id}.json"
                with open(debug_file, 'w') as f:
                    json.dump(offers_response, f, indent=2)
                print(f"📄 Offers debug data saved to: {debug_file}")
        except Exception as e:
            print(f"⚠️  Offers API not supported or failed: {e}")
            # Try to extract seller info from product data
            offers_data = _safe_get(product_response, "offers", default=[])
        
        # Process primary offer (first offer)
        primary_offer = offers_data[0] if offers_data else {}
        normalized_offer = normalize_offer(primary_offer)
        
        # Enrich seller data
        seller_id = normalized_offer.get("seller_id", "")
        seller_url = normalized_offer.get("seller_profile_url", "")
        
        enriched_seller = {}
        if seller_id and _is_numeric_string(seller_id):
            enriched_seller = await enrich_seller_data(
                session, client, seller_id, seller_url, item_id
            )
        else:
            # Try to find seller ID in product data
            numeric_seller_id = _collect_numeric_seller_id(product_response)
            if numeric_seller_id:
                enriched_seller = await enrich_seller_data(
                    session, client, numeric_seller_id, seller_url, item_id
                )
            else:
                # If no seller ID found, create basic seller info from product data
                enriched_seller = {
                    "seller_id": "",
                    "seller_name": "Walmart",
                    "seller_profile_url": "",
                    "business_legal_name": "Walmart Inc.",
                    "email_address": "",
                    "phone_number": "",
                    "address": "",
                    "country": "US",
                    "state_province": "",
                    "zip_code": "",
                    "seller_rating": 0,
                    "seller_profile_picture": "",
                    "total_reviews_seller": 0,
                    "data_source": "product_data",
                    "enrichment_status": "basic"
                }
        
        # Combine all data
        result = {
            **listing,
            **normalized_offer,
            **enriched_seller,
            "offers_count": len(offers_data),
            "units_available": _safe_get(primary_offer, "quantity") or None,  # Don't default to 1 - leave empty if not available
            "keyword": f"item_id_{item_id}",  # For compatibility with existing exports
        }
        
        print(f"✅ Successfully processed item ID: {item_id}")
        return result
        
    except Exception as e:
        print(f"❌ Error processing item ID {item_id}: {e}")
        return None


async def run_id_crawler(item_ids: List[str], export_formats: List[str], 
                        debug: bool = False, sleep: float = 0.5) -> List[Dict[str, Any]]:
    """Run the ID crawler for multiple item IDs."""
    config = get_config()
    client = BlueCartClient(sleep_seconds=sleep)
    
    # Initialize database
    init_db()
    
    # Ensure output directory exists
    ensure_output_dir()
    
    results = []
    
    async with aiohttp.ClientSession() as session:
        for i, item_id in enumerate(item_ids, 1):
            print(f"\n📦 Processing {i}/{len(item_ids)}: {item_id}")
            
            result = await process_item_id(session, client, item_id, debug)
            if result:
                results.append(result)
            
            # Sleep between requests to be respectful
            if i < len(item_ids):
                await asyncio.sleep(sleep)
    
    # Export results
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if "csv" in export_formats:
            csv_file = export_csv(results, f"walmart_id_crawl_{timestamp}")
            print(f"📊 CSV exported: {csv_file}")
        
        if "json" in export_formats:
            json_file = export_json(results, f"walmart_id_crawl_{timestamp}")
            print(f"📄 JSON exported: {json_file}")
        
        print(f"\n🎉 Successfully processed {len(results)}/{len(item_ids)} item IDs")
        print(f"📈 Data enrichment rate: {len([r for r in results if r.get('enrichment_status') == 'success'])}/{len(results)} ({len([r for r in results if r.get('enrichment_status') == 'success'])/len(results)*100:.1f}%)")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Walmart ID Crawler - Direct Item ID Scraping")
    parser.add_argument("--item-ids", type=str, help="Comma-separated list of Walmart item IDs")
    parser.add_argument("--item-ids-file", type=str, help="File containing item IDs (one per line)")
    parser.add_argument("--export", type=str, nargs="+", choices=["csv", "json"], 
                       default=["csv"], help="Export formats")
    parser.add_argument("--debug", action="store_true", help="Save debug JSON files")
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep between requests (seconds)")
    
    args = parser.parse_args()
    
    # Get item IDs
    item_ids = []
    if args.item_ids:
        item_ids = [id.strip() for id in args.item_ids.split(",") if id.strip()]
    elif args.item_ids_file:
        try:
            with open(args.item_ids_file, 'r') as f:
                item_ids = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"❌ File not found: {args.item_ids_file}")
            return 1
    else:
        print("❌ Please provide either --item-ids or --item-ids-file")
        return 1
    
    if not item_ids:
        print("❌ No valid item IDs found")
        return 1
    
    print(f"🚀 Starting Walmart ID Crawler")
    print(f"📋 Item IDs to process: {len(item_ids)}")
    print(f"📤 Export formats: {', '.join(args.export)}")
    print(f"⏱️  Sleep between requests: {args.sleep}s")
    
    start_time = time.time()
    
    # Run the crawler
    results = asyncio.run(run_id_crawler(item_ids, args.export, args.debug, args.sleep))
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n⏱️  Total execution time: {duration:.1f} seconds")
    print(f"📊 Average time per item: {duration/len(item_ids):.1f} seconds")
    
    return 0 if results else 1


if __name__ == "__main__":
    sys.exit(main())
