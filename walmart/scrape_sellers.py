import argparse
import csv
import os
import time
from datetime import datetime
from typing import Dict, List

from seller_enrich import scrape_walmart_seller_profile
from exporters import ensure_output_dir


def find_latest_scan_csv(output_dir: str) -> str:
    candidates: List[str] = []
    for name in os.listdir(output_dir):
        if name.startswith("walmart_scan_") and name.endswith(".csv"):
            candidates.append(os.path.join(output_dir, name))
    if not candidates:
        raise SystemExit("No walmart_scan_*.csv found in output directory")
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def read_unique_profile_urls(csv_path: str) -> List[str]:
    urls: List[str] = []
    seen: set = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = (row.get("seller_profile_url") or "").strip()
            if not u:
                continue
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)
    return urls


def write_results(rows: List[Dict[str, str]], prefix: str) -> str:
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"{prefix}_{ts}.csv")
    headers = [
        "seller_profile_url",
        "seller_profile_picture",
        "business_legal_name",
        "email_address",
        "phone_number",
        "address",
        "country",
        "state_province",
        "zip_code",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Walmart seller profile pages directly (no BlueCart)")
    parser.add_argument("--input", type=str, default="", help="Path to walmart_scan_*.csv. If omitted, latest in output/ is used")
    parser.add_argument("--sleep", type=float, default=0.3, help="Seconds between requests for politeness")
    args = parser.parse_args()

    out_dir = ensure_output_dir()
    scan_csv = args.input.strip() or find_latest_scan_csv(out_dir)
    urls = read_unique_profile_urls(scan_csv)
    if not urls:
        raise SystemExit("No seller_profile_url values found in input CSV")

    print(f"Using input: {scan_csv} | urls={len(urls)}")
    results: List[Dict[str, str]] = []
    for idx, url in enumerate(urls, 1):
        info = scrape_walmart_seller_profile(url) or {}
        row: Dict[str, str] = {"seller_profile_url": url}
        for k in (
            "seller_profile_picture",
            "business_legal_name",
            "email_address",
            "phone_number",
            "address",
            "country",
            "state_province",
            "zip_code",
        ):
            row[k] = info.get(k)
        results.append(row)
        if idx == 1 or idx % 5 == 0:
            print(f"  [{idx}/{len(urls)}] {url} | email={row.get('email_address')} phone={row.get('phone_number')}")
        time.sleep(max(0.0, args.sleep))

    out_path = write_results(results, "sellers_enriched")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()




