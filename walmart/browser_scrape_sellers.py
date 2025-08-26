import argparse
import asyncio
import csv
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from exporters import ensure_output_dir


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(\+?\d[\d\s\-()]{7,}\d)")


def find_latest_scan_csv(output_dir: str) -> str:
    files = [
        os.path.join(output_dir, f)
        for f in os.listdir(output_dir)
        if f.startswith("walmart_scan_") and f.endswith(".csv")
    ]
    if not files:
        raise SystemExit("No walmart_scan_*.csv found")
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def read_unique_profile_urls(csv_path: str) -> List[str]:
    urls: List[str] = []
    seen: set = set()
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u = (row.get("seller_profile_url") or "").strip()
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


def write_results(rows: List[Dict[str, Optional[str]]], prefix: str) -> str:
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"{prefix}_{ts}.csv")
    headers = [
        "seller_profile_url",
        "email_address",
        "phone_number",
        "notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    return path


async def gather_html(playwright, url: str, wait_ms: int = 1500, visible: bool = False, dump_dir: str = "") -> str:
    browser = await playwright.chromium.launch(headless=(not visible))
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
        locale="en-US",
        java_script_enabled=True,
    )
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(wait_ms)
        # If the Walmart bot wall appears, allow manual solve in visible mode
        if "blocked" in (page.url or "") and visible:
            print("Detected Walmart bot wall. Please press & hold in the opened browser window.")
            # Wait up to 120 seconds for challenge to clear
            for _ in range(120):
                await page.wait_for_timeout(1000)
                if "blocked" not in (page.url or ""):
                    break
        html_dom = await page.content()
        try:
            text_dom = await page.evaluate("document.documentElement.innerText")
        except Exception:
            text_dom = ""
        combined = (html_dom or "") + "\n" + (text_dom or "")
        # Try view-source fallback to access raw inline JSON if blocked in DOM
        try:
            await page.goto("view-source:" + url, wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(500)
            # The source is usually rendered inside <pre>
            pre_text = await page.eval_on_selector("pre", "el => el ? el.innerText : ''")
            if pre_text:
                combined += "\n" + pre_text
        except Exception:
            pass
        # Optionally dump to disk
        if dump_dir:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            safe = re.sub(r"[^A-Za-z0-9]+", "_", url)[:120]
            os.makedirs(dump_dir, exist_ok=True)
            path = os.path.join(dump_dir, f"{ts}_{safe}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(combined)
            print(f"Dumped HTML: {path}")
        return combined
    finally:
        await context.close()
        await browser.close()


async def scrape_single(playwright, url: str, visible: bool = False, dump_dir: str = "") -> Dict[str, Optional[str]]:
    html = await gather_html(playwright, url, visible=visible, dump_dir=dump_dir)
    emails = EMAIL_RE.findall(html)
    phones = PHONE_RE.findall(html)
    # follow obvious contact/policy links client-side
    links = re.findall(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.I | re.S)
    for href, text in links[:30]:
        t = re.sub(r"<[^>]+>", " ", text).lower()
        if any(k in t for k in ["contact", "support", "help", "policy", "about", "returns"]):
            try:
                target = href if href.startswith("http") else os.path.join("https://www.walmart.com", href)
                html2 = await gather_html(playwright, target, wait_ms=800)
            except Exception:
                continue
            if not emails:
                emails = EMAIL_RE.findall(html2)
            if not phones:
                phones = PHONE_RE.findall(html2)
            if emails and phones:
                break
    return {
        "seller_profile_url": url,
        "email_address": emails[0] if emails else None,
        "phone_number": phones[0] if phones else None,
        "notes": None if (emails or phones) else "no matches",
    }


async def run(urls: List[str], concurrency: int = 3, visible: bool = False, dump_dir: str = "") -> List[Dict[str, Optional[str]]]:
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        results: List[Dict[str, Optional[str]]] = []
        sem = asyncio.Semaphore(concurrency)

        async def worker(u: str):
            async with sem:
                try:
                    r = await scrape_single(p, u, visible=visible, dump_dir=dump_dir)
                except Exception:
                    r = {"seller_profile_url": u, "email_address": None, "phone_number": None, "notes": "error"}
                results.append(r)

        tasks = [asyncio.create_task(worker(u)) for u in urls]
        await asyncio.gather(*tasks)
        return results


async def run_cdp(urls: List[str], cdp_endpoint: str, dump_dir: str = "") -> List[Dict[str, Optional[str]]]:
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(cdp_endpoint)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()
        results: List[Dict[str, Optional[str]]] = []
        for u in urls:
            try:
                await page.goto(u, wait_until="domcontentloaded", timeout=45000)
                await page.wait_for_timeout(1200)
                if "blocked" in (page.url or ""):
                    print("Bot wall detected in your Chrome session. Please press & hold; I'll wait up to 2 minutes...")
                    for _ in range(120):
                        await page.wait_for_timeout(1000)
                        if "blocked" not in (page.url or ""):
                            break
                html = await page.content()
                try:
                    text_dom = await page.evaluate("document.documentElement.innerText")
                except Exception:
                    text_dom = ""
                combined = (html or "") + "\n" + (text_dom or "")
                if dump_dir:
                    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                    safe = re.sub(r"[^A-Za-z0-9]+", "_", u)[:120]
                    os.makedirs(dump_dir, exist_ok=True)
                    path = os.path.join(dump_dir, f"{ts}_{safe}.html")
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(combined)
                    print(f"Dumped HTML: {path}")
                emails = EMAIL_RE.findall(combined)
                phones = PHONE_RE.findall(combined)
                results.append({
                    "seller_profile_url": u,
                    "email_address": emails[0] if emails else None,
                    "phone_number": phones[0] if phones else None,
                    "notes": None if (emails or phones) else "no matches",
                })
            except Exception:
                results.append({"seller_profile_url": u, "email_address": None, "phone_number": None, "notes": "error"})
        return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Playwright-based Walmart seller page email/phone scraper")
    parser.add_argument("--input", type=str, default="", help="Path to walmart_scan_*.csv; uses latest if omitted")
    parser.add_argument("--urls", type=str, default="", help="Comma-separated seller profile URLs to scrape")
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--visible", action="store_true", help="Open a visible browser window for manual 'press & hold'")
    parser.add_argument("--cdp", type=str, default="", help="Connect to an already-open Chrome via CDP, e.g. ws://127.0.0.1:9222/devtools/browser/<id>")
    parser.add_argument("--dump-html", action="store_true", help="Dump fetched page source to output/raw_html for offline scan")
    args = parser.parse_args()

    out_dir = ensure_output_dir()
    urls_arg = [u.strip() for u in args.urls.split(",") if u.strip()]
    if urls_arg:
        urls = urls_arg
    else:
        csv_path = args.input.strip() or find_latest_scan_csv(out_dir)
        urls = read_unique_profile_urls(csv_path)
    if not urls:
        raise SystemExit("No seller_profile_url values found")

    dump_dir = ""
    if args.dump_html:
        dump_dir = os.path.join(out_dir, "raw_html")

    if args.cdp:
        print(f"Using existing Chrome via CDP: {args.cdp}")
        results = asyncio.run(run_cdp(urls, args.cdp, dump_dir=dump_dir))
    else:
        print(f"Launching Playwright | urls={len(urls)} | visible={args.visible}")
        conc = 1 if args.visible else args.concurrency
        results = asyncio.run(run(urls, concurrency=conc, visible=args.visible, dump_dir=dump_dir))
    out_path = write_results(results, "sellers_browser_enriched")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()



