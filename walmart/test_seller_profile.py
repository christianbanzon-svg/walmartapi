import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from bluecart_client import BlueCartClient
from config import get_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Test BlueCart seller_profile call and save JSON")
    parser.add_argument("--url", type=str, default="", help="Walmart seller profile URL, e.g., https://www.walmart.com/seller/10087")
    parser.add_argument("--seller-id", type=str, default="", help="Walmart seller id (alternative to URL)")
    args = parser.parse_args()

    cfg = get_config()
    client = BlueCartClient(site=cfg.site)

    url: Optional[str] = args.url.strip() or None
    seller_id: Optional[str] = args.seller_id.strip() or None
    if not url and not seller_id:
        raise SystemExit("Provide --url or --seller-id")

    resp = client.seller_profile(seller_id=seller_id, url=url)
    out_dir = Path(cfg.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"seller_profile_test_{ts}.json"
    out_path.write_text(json.dumps(resp, indent=2), encoding="utf-8")

    # Print small summary for quick inspection
    ri = resp.get("request_info") if isinstance(resp, dict) else None
    message = ri.get("message") if isinstance(ri, dict) else None
    keys = list(resp.keys()) if isinstance(resp, dict) else []
    print(f"Saved: {out_path}")
    print(f"message: {message}")
    print(f"keys: {keys[:15]}")


if __name__ == "__main__":
    main()



