# BlueCart API - Seller Information Guide

## Available Methods to Get Seller Information

### 1. **From Search Results (Fastest - Already Available)**
When you call `client.search()`, seller information is included in the response:

```python
search_result = client.search("Holley car", page=1)
items = search_result.get("search_results") or []

for item in items:
    # Seller info is in the offers section
    offers = item.get("offers", {})
    primary_offer = offers.get("primary", {})
    
    # Seller data available:
    seller_name = primary_offer.get("seller_name")
    seller_id = primary_offer.get("seller_id")
    seller_url = primary_offer.get("url")  # Seller profile URL
    
    # Or nested in seller object:
    seller = primary_offer.get("seller", {})
    seller_name = seller.get("name")
    seller_id = seller.get("id")
    seller_url = seller.get("url")
```

**Available fields:**
- `seller_name` / `seller.get("name")`
- `seller_id` / `seller.get("id")`
- `seller_url` / `seller.get("url")`
- `seller_rating` (sometimes)
- `seller_reviews` (sometimes)

### 2. **From Product API (More Details)**
When you call `client.product(item_id)`, it includes seller info:

```python
product_data = client.product("123456789")
offers = product_data.get("offers", [])

if offers:
    primary_offer = offers[0]
    seller_name = primary_offer.get("seller_name")
    seller_id = primary_offer.get("seller_id")
    seller_url = primary_offer.get("url")
```

### 3. **From Offers API (Most Complete)**
When you call `client.offers(item_id)`, it returns all offers with seller details:

```python
offers_data = client.offers("123456789")
offers = offers_data.get("offers", [])

for offer in offers:
    seller_name = offer.get("seller_name")
    seller_id = offer.get("seller_id")
    seller_url = offer.get("url")
    price = offer.get("price")
    # etc.
```

### 4. **Seller Profile API (Currently Not Working)**
The dedicated seller profile endpoint for detailed contact info:

```python
# This is currently timing out - API not responding
seller_profile = client.seller_profile(
    seller_id="F55CDC31AB754BB68FE0B39041159D63"
    # OR
    # url="https://www.walmart.com/seller/101007478"
)
```

**Expected fields (when working):**
- `seller_details.email` or `seller.email`
- `seller_details.phone` or `seller.phone`
- `seller_details.address` or `seller.address`
- `seller_details.business_name` or `seller.business_name`
- `seller_details.country`, `state`, `zip_code`
- `seller_details.rating`
- `seller_details.reviews_count`

## Current Status

✅ **Working:**
- Seller name, ID, and URL from search/product/offers APIs
- Basic seller information is available immediately

❌ **Not Working:**
- `seller_profile` API endpoint (timing out after 30-60+ seconds)
- Contact information (email, phone, address) enrichment

## Recommendation

**Use seller information from search/product/offers responses** - it's:
- ✅ Fast (no extra API calls)
- ✅ Reliable (always available)
- ✅ Includes: name, ID, URL, sometimes rating

**Skip seller_profile API** until BlueCart fixes the timeout issue.

## What We're Currently Getting

From the CSV exports, we're already getting:
- ✅ Seller's Name
- ✅ Seller's URL
- ❌ Seller's Email (requires seller_profile API - not working)
- ❌ Seller's Phone (requires seller_profile API - not working)
- ❌ Seller's Address (requires seller_profile API - not working)

The basic seller information (name, URL) is sufficient for most use cases and is already being collected successfully.








