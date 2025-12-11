# cmc_scrape_async.py
"""
Async Playwright scraper for CoinMarketCap (Top N coins).
Extracts: rank, name, symbol, price, 24h change, market cap, 24h volume, circulating supply.
Saves results to: CSV, TXT, and Excel (.xlsx).

Requirements:
    pip install playwright pandas openpyxl
    python -m playwright install
Run:
    python cmc_scrape_async.py
"""

import asyncio
import csv
import re
import logging
import time
from typing import List, Dict, Optional

import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------
# Configuration
# ---------------------------
TARGET_COUNT = 20
START_URL = "https://coinmarketcap.com/"
OUTPUT_CSV = "coinmarketcap_top20_async.csv"
OUTPUT_TXT = "coinmarketcap_top20_async.txt"
OUTPUT_XLSX = "coinmarketcap_top20_async.xlsx"
HEADLESS = True
MAX_PAGE_NAVIGATIONS = 5   # safety: don't paginate indefinitely
WAIT_TIMEOUT = 15000       # milliseconds
ROW_VISIBLE_TIMEOUT = 2000  # ms

# ---------------------------
# Logging
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("cmc_async")

# ---------------------------
# Helpers
# ---------------------------
def clean_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", s).strip()

async def safe_text(locator, timeout=500) -> str:
    """Return text_content of locator or empty string (with short timeout)."""
    try:
        txt = await locator.text_content(timeout=timeout)
        return clean_text(txt)
    except Exception:
        return ""

def likely_money(s: str) -> bool:
    return bool(s and "$" in s)

def likely_percent(s: str) -> bool:
    return bool(s and "%" in s)

# extraction with safe fallbacks
async def extract_from_row(row) -> Dict[str, str]:
    """
    Given a Playwright locator pointing to a <tr>, attempt to extract:
    rank, name, symbol, price, change_24h, market_cap, volume_24h, circulating_supply
    """
    data = {
        "rank": "",
        "name": "",
        "symbol": "",
        "price": "",
        "change_24h": "",
        "market_cap": "",
        "volume_24h": "",
        "circulating_supply": ""
    }
    try:
        # Collect all td texts to analyze fallback positions
        tds = []
        try:
            td_count = await row.locator("td").count()
            for i in range(td_count):
                txt = await safe_text(row.locator("td").nth(i))
                tds.append(clean_text(txt))
        except Exception:
            tds = []

        # 1) RANK: usually first td and often a simple number
        if tds:
            # first non-empty number-like piece
            for t in tds[:3]:
                m = re.match(r"^\s*(\d{1,4})\b", t)
                if m:
                    data["rank"] = m.group(1)
                    break

        # 2) NAME & SYMBOL:
        # - CoinMarketCap often contains the name and symbol inside the same cell:
        #   name in a <p> and symbol in a small or span. We'll try to locate typical sub-elements.
        try:
            # try anchor to currency page (has href '/currencies/')
            name_locator = row.locator("a[href*='/currencies/'] >> h3, a[href*='/currencies/'] >> p")
            if await name_locator.count() > 0:
                # prefer a header-like element for name
                name_text = clean_text(await name_locator.nth(0).text_content())
                if name_text:
                    data["name"] = name_text
            # symbol: often a small tag or span with 3-6 chars, near the name
            sym_loc = row.locator("a[href*='/currencies/'] >> div >> p > span, a[href*='/currencies/'] >> p + p, a[href*='/currencies/'] >> span")
            if await sym_loc.count() > 0:
                sym = clean_text(await sym_loc.nth(0).text_content())
                # some cleaning: remove parentheses etc
                sym = re.sub(r"[\(\)]", "", sym)
                if 1 < len(sym) <= 10:
                    data["symbol"] = sym
        except Exception:
            pass

        # fallback: try to extract name & symbol from tds content heuristics
        if not data["name"] or not data["symbol"]:
            for t in tds:
                # often format: "Bitcoin BTC" or "Bitcoin\nBTC"
                if t and re.search(r"[A-Za-z]", t):
                    # try capture "Name SYMBOL"
                    parts = re.split(r"\s{2,}|\s?\n\s?|\s•\s", t)
                    # pick the part with letters and length >1
                    for p in parts:
                        p = p.strip()
                        if len(p) > 1 and not likely_money(p) and not likely_percent(p):
                            # if contains uppercase short token at end, treat as symbol
                            m = re.match(r"^(.+?)\s+([A-Z]{2,6})$", p)
                            if m:
                                data["name"] = data["name"] or m.group(1).strip()
                                data["symbol"] = data["symbol"] or m.group(2).strip()
                                break
                            # if single word uppercase, may be symbol
                            if p.isupper() and 2 <= len(p) <= 6:
                                data["symbol"] = data["symbol"] or p
                            elif len(p.split()) <= 3 and not data["name"]:
                                data["name"] = p

        # 3) PRICE: look for first $ string in tds
        if tds:
            for t in tds:
                if "$" in t and len(t) < 20:
                    # avoid picking market cap (long numbers) — price strings usually short
                    data["price"] = t
                    break
        # fallback: try common selector for price
        if not data["price"]:
            try:
                price_sel = row.locator("td >> a[href*='/price/'], td >> span:has-text('$')")
                if await price_sel.count() > 0:
                    data["price"] = clean_text(await price_sel.nth(0).text_content())
            except Exception:
                pass

        # 4) 24H change: pick td with '%' text
        if tds:
            for t in tds:
                if "%" in t and re.search(r"-?\d+(\.\d+)?%", t):
                    data["change_24h"] = t
                    break

        # 5) MARKET CAP: choose $ value with larger magnitude or labeled 'Market Cap'
        if tds:
            candidates = [t for t in tds if "$" in t and len(t) > 6]
            if candidates:
                # pick the longest $ string (heuristic for market cap)
                candidates.sort(key=lambda s: len(s), reverse=True)
                data["market_cap"] = candidates[0]

        # 6) 24H VOLUME: often a $ value but smaller than market cap; try to find second-largest $-value
        if tds:
            dollar_values = [t for t in tds if "$" in t]
            if dollar_values:
                # try to choose volume as the second-longest (if multiple exist)
                sorted_dollars = sorted(dollar_values, key=lambda s: len(s), reverse=True)
                if len(sorted_dollars) >= 2:
                    data["volume_24h"] = sorted_dollars[1]
                elif len(sorted_dollars) == 1:
                    # maybe volume not present or same as market cap; leave blank
                    data["volume_24h"] = ""

        # 7) CIRCULATING SUPPLY: often contains tokens and maybe symbol, like "19,000,000 BTC"
        circ = ""
        for t in tds:
            if t and re.search(r"\d[\d,\.]*\s*[A-Za-z]{1,6}", t) and "Market Cap" not in t and "$" not in t:
                # choose the one with numbers and a short symbol
                circ = t
                break
        data["circulating_supply"] = circ

    except Exception as e:
        logger.exception("extract_from_row exception: %s", e)

    # final cleanup: ensure strings
    for k in data:
        data[k] = clean_text(data[k])
    return data

# ---------------------------
# Main scraper (async)
# ---------------------------
async def scrape_top_n(target_count=TARGET_COUNT) -> List[Dict[str,str]]:
    results: List[Dict[str,str]] = []
    navigations = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context()
        page = await context.new_page()
        logger.info("Opening %s", START_URL)
        try:
            await page.goto(START_URL, timeout=WAIT_TIMEOUT)
            await page.wait_for_selector("tbody tr", timeout=WAIT_TIMEOUT)
        except PlaywrightTimeoutError:
            logger.warning("Timed out waiting for table rows. The site may be slow or layout may have changed.")
        except Exception as e:
            logger.exception("Error loading page: %s", e)
            await browser.close()
            return results

        # small settle
        await asyncio.sleep(1.0)

        while len(results) < target_count and navigations < MAX_PAGE_NAVIGATIONS:
            navigations += 1
            logger.info("Scanning page (nav=%d). Collected so far: %d", navigations, len(results))

            try:
                rows = page.locator("tbody tr")
                row_count = await rows.count()
            except Exception as e:
                logger.exception("Failed to locate rows: %s", e)
                row_count = 0

            logger.info("Found %d rows on page", row_count)

            for i in range(row_count):
                if len(results) >= target_count:
                    break
                try:
                    row = rows.nth(i)
                    # wait briefly for row visibility (best effort)
                    try:
                        await row.wait_for(state="visible", timeout=ROW_VISIBLE_TIMEOUT)
                    except Exception:
                        pass

                    data = await extract_from_row(row)
                    # validation: must have name and price or rank
                    if not data["name"] and not data["price"] and not data["rank"]:
                        continue

                    # dedupe by rank or name
                    if data["rank"] and any(r.get("rank") == data["rank"] for r in results):
                        continue
                    if data["name"] and any(r.get("name") == data["name"] for r in results):
                        continue

                    logger.info("Collected: %s | %s | %s | %s", data.get("rank"), data.get("name"), data.get("symbol"), data.get("price"))
                    results.append(data)
                except Exception as e:
                    logger.exception("Error processing row %d: %s", i, e)
                    continue

            # pagination if needed
            if len(results) < target_count:
                try:
                    next_btn = page.locator("button[aria-label='Next'], a[aria-label='Next']")
                    if await next_btn.count() > 0 and await next_btn.is_enabled():
                        logger.info("Clicking Next to paginate...")
                        await next_btn.first.click()
                        # wait for new rows to appear
                        await page.wait_for_selector("tbody tr", timeout=WAIT_TIMEOUT)
                        await asyncio.sleep(1.0)
                        continue
                    else:
                        logger.info("No Next button found or it's disabled. Stopping.")
                        break
                except Exception as e:
                    logger.warning("Pagination attempt failed: %s", e)
                    break

        await browser.close()

    # trim to target_count
    return results[:target_count]

# ---------------------------
# Save functions
# ---------------------------
def save_to_csv(records: List[Dict[str,str]], path: str):
    if not records:
        logger.warning("No records to save to CSV.")
        return
    keys = ["rank", "name", "symbol", "price", "change_24h", "market_cap", "volume_24h", "circulating_supply"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in records:
            writer.writerow({k: r.get(k, "") for k in keys})
    logger.info("Saved %d records to CSV: %s", len(records), path)

def save_to_text(records: List[Dict[str,str]], path: str):
    if not records:
        logger.warning("No records to save to text.")
        return
    with open(path, "w", encoding="utf-8") as f:
        for idx, r in enumerate(records, start=1):
            f.write(f"{idx}. Rank: {r.get('rank','')}\n")
            f.write(f"   Name: {r.get('name','')} ({r.get('symbol','')})\n")
            f.write(f"   Price: {r.get('price','')}\n")
            f.write(f"   24h Change: {r.get('change_24h','')}\n")
            f.write(f"   Market Cap: {r.get('market_cap','')}\n")
            f.write(f"   24h Volume: {r.get('volume_24h','')}\n")
            f.write(f"   Circulating Supply: {r.get('circulating_supply','')}\n")
            f.write("\n")
    logger.info("Saved %d records to TXT: %s", len(records), path)

def save_to_excel(records: List[Dict[str,str]], path: str):
    if not records:
        logger.warning("No records to save to Excel.")
        return
    df = pd.DataFrame(records)
    # reorder columns
    cols = ["rank", "name", "symbol", "price", "change_24h", "market_cap", "volume_24h", "circulating_supply"]
    df = df.reindex(columns=cols)
    df.to_excel(path, index=False)
    logger.info("Saved %d records to Excel: %s", len(records), path)

# ---------------------------
# Entrypoint
# ---------------------------
async def main():
    logger.info("Starting async CoinMarketCap scraper for top %d", TARGET_COUNT)
    try:
        records = await scrape_top_n(TARGET_COUNT)
        if not records:
            logger.error("No records scraped. Exiting.")
            return

        save_to_csv(records, OUTPUT_CSV)
        save_to_text(records, OUTPUT_TXT)
        save_to_excel(records, OUTPUT_XLSX)
        logger.info("Scraping and saving finished successfully.")
    except Exception as e:
        logger.exception("Fatal error during scraping: %s", e)

if __name__ == "__main__":
    asyncio.run(main())
