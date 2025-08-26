import argparse
import csv
import os
from datetime import datetime
from typing import Dict, List, Optional, Set

from exporters import ensure_output_dir
from bluecart_client import BlueCartClient


def _safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def extract_seller_urls_from_search(resp: Dict) -> List[str]:
    urls: Set[str] = set()
    if not isinstance(resp, dict):
        return []
    items = resp.get("search_results") or resp.get("items") or []
    if not items and isinstance(resp.get("data"), dict):
        data = resp["data"]
        items = data.get("search_results") or data.get("items") or data.get("results") or []
    for it in items:
        # Prefer explicit seller profile url if present
        u = (it.get("seller_profile_url") or it.get("sellerUrl") or "").strip()
        if u:
            urls.add(u)
            continue
        # Build from seller id if available
        sid = (it.get("seller_id") or it.get("sellerId") or "").strip()
        if sid:
            urls.add(f"https://www.walmart.com/seller/{sid}")
            continue
        # Try common BlueCart nesting: offers.primary.seller{_id,url}
        sid2 = _safe_get(it, "offers", "primary", "seller_id") or _safe_get(it, "offers", "primary", "seller", "id")
        surl2 = _safe_get(it, "offers", "primary", "seller_url") or _safe_get(it, "offers", "primary", "seller", "url")
        if isinstance(surl2, str) and surl2:
            urls.add(surl2)
            continue
        if isinstance(sid2, str) and sid2:
            urls.add(f"https://www.walmart.com/seller/{sid2}")
    return sorted(urls)


def write_csv(urls: List[str]) -> str:
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"seller_urls_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["seller_profile_url"]) 
        for u in urls:
            w.writerow([u])
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect Walmart seller profile URLs via BlueCart search")
    parser.add_argument("--keywords", type=str, default="instyler", help="Comma-separated keywords")
    parser.add_argument("--pages", type=int, default=1, help="Pages per keyword")
    args = parser.parse_args()

    client = BlueCartClient()
    gathered: Set[str] = set()
    for kw in [s.strip() for s in args.keywords.split(",") if s.strip()]:
        for p in range(1, args.pages + 1):
            resp = client.search(kw, page=p)
            urls = extract_seller_urls_from_search(resp)
            for u in urls:
                gathered.add(u)
    path = write_csv(sorted(gathered))
    print(f"Saved: {path}")


if __name__ == "__main__":
    main()


