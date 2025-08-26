import argparse
import csv
import os
from datetime import datetime
from typing import Dict, List

from bluecart_client import BlueCartClient
from exporters import ensure_output_dir


def _is_numeric(s: str) -> bool:
    try:
        return s is not None and str(int(str(s))) == str(s)
    except Exception:
        return False


def _collect_numeric_seller_ids(node, out: Dict[str, str]) -> None:
    if isinstance(node, dict):
        # seller object with id
        if "seller" in node and isinstance(node["seller"], dict):
            sid = node["seller"].get("id")
            name = node["seller"].get("name") or node.get("seller_name") or ""
            if isinstance(sid, (str, int)) and _is_numeric(str(sid)):
                out[str(sid)] = name
        # direct seller_id
        sid2 = node.get("seller_id") or node.get("sellerId")
        name2 = node.get("seller_name") or node.get("sellerName") or node.get("name") or ""
        if isinstance(sid2, (str, int)) and _is_numeric(str(sid2)):
            out[str(sid2)] = name2
        for v in node.values():
            _collect_numeric_seller_ids(v, out)
    elif isinstance(node, list):
        for it in node:
            _collect_numeric_seller_ids(it, out)


def collect_numeric_sellers(client: BlueCartClient, keyword: str, pages: int) -> Dict[str, str]:
    sellers: Dict[str, str] = {}
    item_ids: List[str] = []
    for p in range(1, pages + 1):
        resp = client.search(keyword, page=p)
        items = resp.get("search_results") or resp.get("items") or []
        if not items and isinstance(resp.get("data"), dict):
            items = resp["data"].get("search_results") or resp["data"].get("items") or []
        for it in items:
            # collect numeric seller ids directly from search payload
            _collect_numeric_seller_ids(it, sellers)
            # remember item ids for deeper product probing
            pid = (it.get("product") or {}).get("item_id") or (it.get("product") or {}).get("product_id")
            if pid:
                item_ids.append(str(pid))
    # probe product endpoint for first few items
    for pid in item_ids[:20]:
        try:
            prod = client.product(pid)
        except Exception:
            continue
        _collect_numeric_seller_ids(prod, sellers)
    return sellers


def run_probe(keyword: str, pages: int, limit: int) -> str:
    client = BlueCartClient()
    sellers = collect_numeric_sellers(client, keyword, pages)
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"numeric_seller_probe_{ts}.csv")
    rows: List[Dict[str, str]] = []
    for sid, name in list(sellers.items())[: limit]:
        try:
            sp = client.seller_profile(seller_id=sid)
            node = sp.get("seller_details") if isinstance(sp, dict) else {}
            email = (node or sp).get("email") if isinstance(node or sp, dict) else ""
            phone = (node or sp).get("phone") if isinstance(node or sp, dict) else ""
            rows.append({"seller_id": sid, "seller_name": name, "email": email or "", "phone": phone or "", "status": "ok"})
        except Exception as e:
            rows.append({"seller_id": sid, "seller_name": name, "email": "", "phone": "", "status": type(e).__name__})
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["seller_id", "seller_name", "email", "phone", "status"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"Saved: {path}")
    return path


def main() -> None:
    ap = argparse.ArgumentParser(description="Probe BlueCart for numeric seller_ids from search and test seller_profile")
    ap.add_argument("--keyword", type=str, default="instyler")
    ap.add_argument("--pages", type=int, default=1)
    ap.add_argument("--limit", type=int, default=6)
    args = ap.parse_args()

    run_probe(args.keyword, args.pages, args.limit)


if __name__ == "__main__":
    main()


