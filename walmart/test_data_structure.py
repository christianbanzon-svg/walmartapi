import os
from bluecart_client import BlueCartClient
from config import get_config

def test_data_structure():
    # Set environment variable
    os.environ["WALMART_DOMAIN"] = "walmart.com"
    
    config = get_config()
    client = BlueCartClient(config.api_key, config.base_url, config.site)
    
    print("Testing BlueCart search response structure...")
    
    try:
        search_resp = client.search("Dell laptop", page=1)
        print("Search response keys:", list(search_resp.keys()))
        
        if "request_info" in search_resp:
            print("Request info:", search_resp["request_info"])
            if not search_resp["request_info"].get("success"):
                return
        
        items = search_resp.get("search_results") or search_resp.get("items") or []
        if not items:
            items = (search_resp.get("results") or [])
        if not items and isinstance(search_resp.get("data"), dict):
            data = search_resp["data"]
            items = data.get("search_results") or data.get("items") or data.get("results") or []
        
        print(f"Found {len(items)} items")
        
        if items:
            print("First item keys:", list(items[0].keys()))
            print("First item sample:")
            import json
            print(json.dumps(items[0], indent=2)[:1000] + "...")
            
            # Check for item_id in different locations
            item = items[0]
            print("\nChecking for item_id:")
            print("item_id:", item.get("item_id"))
            print("id:", item.get("id"))
            print("product.id:", item.get("product", {}).get("id"))
            print("product.item_id:", item.get("product", {}).get("item_id"))
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_data_structure()
