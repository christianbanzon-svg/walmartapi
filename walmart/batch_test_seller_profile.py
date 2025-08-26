import argparse
import csv
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from bluecart_client import BlueCartClient
from config import get_config


def ensure_output_dir() -> str:
    cfg = get_config()
    os.makedirs(cfg.output_dir, exist_ok=True)
    return cfg.output_dir


def extract_seller_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"/seller/([^/?#]+)", url)
    if not m:
        return None
    return m.group(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch test BlueCart seller_profile for multiple sellers")
    parser.add_argument("--input", type=str, default="", help="CSV with seller_profile_url column")
    parser.add_argument("--urls", type=str, default="", help="Comma-separated seller profile URLs")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--sleep", type=float, default=1.0)
    args = parser.parse_args()

    out_dir = ensure_output_dir()
    urls: List[str] = []
    if args.urls.strip():
        urls = [u.strip() for u in args.urls.split(",") if u.strip()]
    elif args.input.strip():
        with open(args.input, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                u = (row.get("seller_profile_url") or "").strip()
                if u:
                    urls.append(u)
    else:
        # try to locate the latest seller_urls_*.csv in output
        files = [os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.startswith("seller_urls_") and f.endswith(".csv")]
        if files:
            files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            with open(files[0], newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    u = (row.get("seller_profile_url") or "").strip()
                    if u:
                        urls.append(u)
    urls = urls[: args.limit]
    if not urls:
        raise SystemExit("No seller URLs found")

    client = BlueCartClient(request_timeout_seconds=120.0, max_retries=5, retry_backoff_seconds=2.5)
    results: List[Dict[str, Optional[str]]] = []
    for u in urls:
        sid = extract_seller_id_from_url(u)
        try:
            # Prefer seller_id when numeric; otherwise pass URL to API
            payload_id = sid if (sid and sid.isdigit()) else None
            resp = client.seller_profile(seller_id=payload_id, url=None if payload_id else u)
            node = resp.get("seller_details") if isinstance(resp, dict) else {}
            row = {
                "seller_profile_url": u,
                "seller_id_param": payload_id or "",
                "email": (node or resp).get("email") if isinstance(node or resp, dict) else "",
                "phone": (node or resp).get("phone") if isinstance(node or resp, dict) else "",
                "name": (node or resp).get("name") if isinstance(node or resp, dict) else "",
                "status_msg": (resp.get("request_info", {}) or {}).get("message") if isinstance(resp, dict) else "",
            }
        except Exception as e:
            row = {
                "seller_profile_url": u,
                "seller_id_param": payload_id or "",
                "email": "",
                "phone": "",
                "name": "",
                "status_msg": f"error: {type(e).__name__}",
            }
        results.append(row)
        time.sleep(args.sleep)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"seller_profile_batch_{ts}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["seller_profile_url", "seller_id_param", "email", "phone", "name", "status_msg"])
        w.writeheader()
        for r in results:
            w.writerow(r)
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()


