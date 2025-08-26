import argparse
import asyncio
import csv
import os
import re
import time
from datetime import datetime
from typing import Dict, List, Optional

from urllib.parse import quote_plus, urljoin

from exporters import ensure_output_dir

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(\+?\d[\d\s\-()]{7,}\d)")


async def get_html(playwright, url: str, wait_ms: int = 1500) -> str:
    browser = await playwright.chromium.launch(headless=True)
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
        return await page.content()
    finally:
        await context.close()
        await browser.close()


async def open_page(playwright, headless: bool = True):
    # Launch with a few flags that commonly reduce bot challenges
    browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-site-isolation-trials",
        ],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        ),
        locale="en-US",
        java_script_enabled=True,
        viewport={"width": 1366, "height": 900},
    )
    # Hide webdriver
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
    )
    page = await context.new_page()
    return browser, context, page


def _looks_like_challenge(html: str) -> bool:
    text = (html or "").lower()
    return ("robot or human" in text) or ("are you a human" in text) or ("blocked" in text)


async def collect_product_links(page, search_url: str, max_links: int = 10) -> List[str]:
    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
    await page.wait_for_timeout(1500)
    # Quick challenge detection and one retry
    html0 = await page.content()
    if _looks_like_challenge(html0):
        await page.wait_for_timeout(2500)
        await page.reload(wait_until="domcontentloaded")
        await page.wait_for_timeout(1500)
    # Try to wait for typical product anchors to appear
    try:
        await page.wait_for_selector('a[href*="/ip/"]', timeout=12000)
    except Exception:
        # Scroll a bit to trigger lazy content
        try:
            await page.evaluate("window.scrollBy(0, 1200)")
            await page.wait_for_timeout(800)
        except Exception:
            pass
    # Progressive scroll to force render of more items
    try:
        for _ in range(4):
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            await page.wait_for_timeout(700)
    except Exception:
        pass
    # Primary selector
    links_primary = await page.eval_on_selector_all(
        'a[href*="/ip/"]',
        "els => els.map(e => e.href).filter((v,i,a)=>a.indexOf(v)===i)"
    )
    if not isinstance(links_primary, list):
        links_primary = []
    # Fallback: product title anchors sometimes use testing/automation ids
    links_fallback = await page.evaluate(
        """
        () => {
          const els = Array.from(document.querySelectorAll('a[data-automation-id="product-title"], a[data-testid="product-title"]'));
          const hrefs = els.map(e => e.href || e.closest('a')?.href).filter(Boolean);
          return Array.from(new Set(hrefs));
        }
        """
    )
    links = list(dict.fromkeys([*(links_primary or []), *(links_fallback or [])]))
    uniq = [l for l in links if l and "/ip/" in l]
    return uniq[:max_links]


async def find_seller_url_on_product(page) -> Optional[str]:
    # Common: an anchor with /seller/ in href
    seller = await page.eval_on_selector(
        'a[href*="/seller/"]',
        "el => el ? el.href : null"
    )
    if seller:
        return seller
    # Some products embed seller link in button
    seller = await page.eval_on_selector(
        '[data-automation-id="seller-name"] a, [data-testid="seller-name"] a',
        "el => el ? el.href : null"
    )
    return seller


def extract_contacts_from_html(html: str) -> Dict[str, Optional[str]]:
    emails = EMAIL_RE.findall(html)
    phones = PHONE_RE.findall(html)
    return {
        "email_address": emails[0] if emails else None,
        "phone_number": phones[0] if phones else None,
    }


async def run_flow(keywords: List[str], max_products: int = 6) -> List[Dict[str, Optional[str]]]:
    from playwright.async_api import async_playwright

    async def maybe_solve_press_hold(page) -> None:
        try:
            if "blocked" in (page.url or ""):
                # Try to find the PRESS & HOLD control
                btn = page.locator('text="PRESS & HOLD"')
                if await btn.count() == 0:
                    btn = page.locator('button:has-text("PRESS & HOLD")')
                if await btn.count() > 0:
                    box = await btn.bounding_box()
                    if box:
                        await page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                        await page.mouse.down()
                        await page.wait_for_timeout(6500)
                        await page.mouse.up()
                        # Give page time to release challenge and redirect
                        await page.wait_for_timeout(2000)
        except Exception:
            pass

    results: List[Dict[str, Optional[str]]] = []
    async with async_playwright() as p:
        browser, context, page = await open_page(p, headless=False)
        try:
            for kw in keywords:
                # Human-like flow: open homepage, type query, press Enter
                await page.goto("https://www.walmart.com/", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(1200)
                await maybe_solve_press_hold(page)
                # Try closing any region/location modals
                try:
                    await page.click('button:has-text("Got it")', timeout=2000)
                except Exception:
                    pass
                # Focus search box (several selectors depending on layout)
                focused = False
                for sel in [
                    'input[aria-label="Search Walmart.com"]',
                    'input[aria-label="Search"]',
                    'input[data-automation-id="search-form-input"]',
                    'input[name="q"]'
                ]:
                    try:
                        await page.focus(sel)
                        await page.fill(sel, kw)
                        focused = True
                        break
                    except Exception:
                        continue
                if not focused:
                    # Fallback to URL-based search
                    await page.goto(f"https://www.walmart.com/search?q={quote_plus(kw)}", wait_until="domcontentloaded", timeout=30000)
                else:
                    await page.keyboard.press("Enter")
                await page.wait_for_timeout(1500)
                await maybe_solve_press_hold(page)
                # Now collect links from current page
                prod_links = await collect_product_links(page, page.url, max_links=max_products)
                # If no links, record and continue
                if not prod_links:
                    results.append({
                        "keyword": kw,
                        "product_url": None,
                        "seller_profile_url": None,
                        "email_address": None,
                        "phone_number": None,
                        "notes": "no product links"
                    })
                    continue
                for pl in prod_links:
                    try:
                        await page.goto(pl, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(1200)
                        # Challenge check on product page
                        html_prod = await page.content()
                        if _looks_like_challenge(html_prod):
                            await maybe_solve_press_hold(page)
                            await page.wait_for_timeout(2000)
                            await page.reload(wait_until="domcontentloaded")
                            await page.wait_for_timeout(1000)
                        seller_url = await find_seller_url_on_product(page)
                        if not seller_url:
                            results.append({
                                "keyword": kw,
                                "product_url": pl,
                                "seller_profile_url": None,
                                "email_address": None,
                                "phone_number": None,
                                "notes": "no seller link"
                            })
                            continue
                        await page.goto(seller_url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(1200)
                        html = await page.content()
                        if _looks_like_challenge(html):
                            await maybe_solve_press_hold(page)
                            await page.wait_for_timeout(2000)
                            await page.reload(wait_until="domcontentloaded")
                            await page.wait_for_timeout(1000)
                            html = await page.content()
                        contacts = extract_contacts_from_html(html)
                        # follow likely links if empty
                        if not contacts["email_address"] or not contacts["phone_number"]:
                            links = await page.eval_on_selector_all(
                                'a[href]',
                                "els => els.map(e => ({href:e.href, text:e.innerText?.toLowerCase()||''}))"
                            )
                            for entry in links[:30]:
                                t = entry.get("text") or ""
                                if any(k in t for k in ["contact", "support", "help", "policy", "return", "about"]):
                                    try:
                                        await page.goto(entry["href"], wait_until="domcontentloaded", timeout=25000)
                                        await page.wait_for_timeout(800)
                                        html2 = await page.content()
                                        c2 = extract_contacts_from_html(html2)
                                        for k in ("email_address", "phone_number"):
                                            if not contacts.get(k) and c2.get(k):
                                                contacts[k] = c2[k]
                                        if contacts["email_address"] or contacts["phone_number"]:
                                            break
                                    except Exception:
                                        pass
                        results.append({
                            "keyword": kw,
                            "product_url": pl,
                            "seller_profile_url": seller_url,
                            "email_address": contacts.get("email_address"),
                            "phone_number": contacts.get("phone_number"),
                            "notes": None if (contacts.get("email_address") or contacts.get("phone_number")) else "no matches"
                        })
                    except Exception:
                        results.append({
                            "keyword": kw,
                            "product_url": pl,
                            "seller_profile_url": None,
                            "email_address": None,
                            "phone_number": None,
                            "notes": "error"
                        })
        finally:
            await context.close()
            await browser.close()
    return results


def write_output(rows: List[Dict[str, Optional[str]]]) -> str:
    out_dir = ensure_output_dir()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(out_dir, f"search_to_seller_{ts}.csv")
    headers = ["keyword", "product_url", "seller_profile_url", "email_address", "phone_number", "notes"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Search Walmart, open product, visit seller page, extract contacts (Playwright)")
    parser.add_argument("--keywords", type=str, default="instyler", help="Comma-separated keywords")
    parser.add_argument("--max-products", type=int, default=5)
    args = parser.parse_args()

    keywords = [s.strip() for s in args.keywords.split(",") if s.strip()]
    rows = asyncio.run(run_flow(keywords, max_products=args.max_products))
    out = write_output(rows)
    print(f"Saved: {out}")


if __name__ == "__main__":
    main()



