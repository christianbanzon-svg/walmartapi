import argparse
import csv
import os
import re
from datetime import datetime
from typing import List, Dict, Optional

import requests

from bluecart_client import BlueCartClient
from exporters import ensure_output_dir


DDG_HTML = "https://duckduckgo.com/html/"
WALMART_CA = "walmart.ca"


def ddg_search(query: str, max_results: int = 20) -> List[str]:
    params = {"q": query}
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(DDG_HTML, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    html = r.text
    # crude extraction of Walmart.ca links
    links = re.findall(r"https?://www\.walmart\.ca/ip/[^\s'\"]+", html)
    # normalize and dedupe
    seen = set()
    out: List[str] = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
        if len(out) >= max_results:
            break
    return out


def extract_item_id(url: str) -> Optional[str]:
    # common forms: /ip/<slug>/<digits>
    m = re.search(r"/ip/[^/]+/(\d+)", url)
    if m:
        return m.group(1)
    # sometimes just /ip/(digits)
    m = re.search(r"/ip/(\d+)", url)
    if m:
        return m.group(1)
    return None


def write_csv(rows: List[Dict[str, str]]) -> str:
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"walmart_ca_seed_{ts}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["keyword", "item_id", "seller_id", "seller_name", "email", "phone", "status"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="Seed Walmart.ca via DuckDuckGo, then call BlueCart product+seller_profile")
    ap.add_argument("--keyword", type=str, default="nike")
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    # Force Walmart domain to CA for BlueCart calls
    client = BlueCartClient(site="walmart.ca", request_timeout_seconds=60.0)

    query = f"site:{WALMART_CA}/ip {args.keyword}"
    urls = ddg_search(query, max_results=args.limit * 2)
    rows: List[Dict[str, str]] = []

    seen_items = set()
    for u in urls:
        item_id = extract_item_id(u)
        if not item_id or item_id in seen_items:
            continue
        seen_items.add(item_id)
        try:
            prod = client.product(item_id)
            # collect numeric seller ids in the product payload
            seller_id: Optional[str] = None
            seller_name: str = ""
            offers = prod.get("offers") or prod.get("product", {}).get("offers") or {}
            primary = offers.get("primary") or {}
            sid = primary.get("seller_id") or (primary.get("seller") or {}).get("id")
            sname = primary.get("seller_name") or (primary.get("seller") or {}).get("name")
            if isinstance(sid, (str, int)) and str(sid).isdigit():
                seller_id = str(sid)
                seller_name = sname or ""
            email = ""; phone = ""; status = ""
            if seller_id:
                sp = client.seller_profile(seller_id=seller_id)
                node = sp.get("seller_details") if isinstance(sp, dict) else {}
                if isinstance(node, dict):
                    email = node.get("email") or ""
                    phone = node.get("phone") or ""
            else:
                status = "no_numeric_sid"
            rows.append({
                "keyword": args.keyword,
                "item_id": item_id,
                "seller_id": seller_id or "",
                "seller_name": seller_name,
                "email": email,
                "phone": phone,
                "status": status or ("ok" if email or phone else "no_contact"),
            })
            if len(rows) >= args.limit:
                break
        except Exception:
            rows.append({
                "keyword": args.keyword,
                "item_id": item_id or "",
                "seller_id": "",
                "seller_name": "",
                "email": "",
                "phone": "",
                "status": "error",
            })
            continue

    out = write_csv(rows)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()



