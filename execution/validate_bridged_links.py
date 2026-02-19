
import asyncio
import pandas as pd
import os
import re
import random
import sys
import time
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.bridge_utils import safe_goto
from parallel_fallback_recovery import fallback_search_amazon, get_new_page

HOTFIX_FILE = "recovered_links_hotfix.csv"
VALIDATED_FILE = "recovered_links_validated.csv"

# Broadened Amazon search landers
SEARCH_PATTERNS = ["/s?k=", "s-k=", "keywords=", "/s/ref=", "field-keywords"]

async def resolve_to_dp_link(context, page, amz_link, title, author):
    """
    Checks if an Amazon link is a direct product (DP) link.
    If it's a search or redirect link, attempts to find the actual DP link.
    """
    try:
        if not amz_link or str(amz_link) == 'nan': return None
        
        # 0. If it's already a clean DP link, return it
        if "/dp/" in amz_link or "/gp/product/" in amz_link:
            clean = amz_link.split("?")[0]
            # Verify it's actually Amazon
            if "amazon.com" in clean:
                return clean
        
        # 1. Follow the redirect/link to see where it lands
        logger.debug(f"  - Checking link: {amz_link}")
        if await safe_goto(page, amz_link):
            final_url = page.url
            
            # Check for direct product landing
            if "/dp/" in final_url or "/gp/product/" in final_url:
                clean_url = final_url.split("?")[0]
                logger.success(f"    ✓ Resolved to DP link: {clean_url}")
                return clean_url
            
            # Check for search landing
            elif any(pat in final_url for pat in SEARCH_PATTERNS):
                logger.warning(f"    ⚠ Landed on SEARCH page: {final_url}")
                
                # Robust parsing for the first search result
                selectors = [
                    "div[data-component-type='s-search-result'] h2 a.a-link-normal",
                    "h2 a.a-link-normal",
                    "a.a-link-normal .a-size-medium",
                    "a.a-link-normal .a-size-base-plus"
                ]
                for sel in selectors:
                    first_item = await page.query_selector(sel)
                    if first_item:
                        # If selector hit a span inside the link, traverse up
                        href = await first_item.get_attribute("href")
                        if not href:
                            parent = await first_item.query_selector("xpath=ancestor::a[@href]")
                            if parent:
                                first_item = parent
                                href = await first_item.get_attribute("href")
                        
                        if href and "/dp/" in href:
                            dp_link = "https://www.amazon.com" + href.split("?")[0]
                            logger.success(f"    ✓ Extracted DP from search landing: {dp_link}")
                            return dp_link
            else:
                logger.warning(f"    ⚠ Landed on unknown non-DP page: {final_url}")
        
        # 2. Fallback: High-precision Amazon search
        logger.info(f"    → Resolving to DP via internal search: {title} by {author}")
        search_res = await fallback_search_amazon(context, page, title, author)
        
        if isinstance(search_res, dict) and 'Amazon Link' in search_res:
            dp_link = search_res['Amazon Link'].split("?")[0]
            logger.success(f"    ✓ Resolved to DP (Search): {dp_link}")
            return dp_link
            
        return None
    except Exception as e:
        logger.error(f"    Error resolving link for {title}: {e}")
        return None

async def validation_worker(worker_id, browser, queue, results_dict):
    """Concurrent worker for link resolution."""
    context, page = await get_new_page(browser)
    try:
        while not queue.empty():
            try:
                idx, row = await queue.get()
            except asyncio.QueueEmpty:
                break
                
            title = row.get('Title', 'Unknown Title')
            author = row.get('Author Name', '')
            amz_link = row.get('Amazon Link', '')
            
            logger.info(f"[Worker {worker_id}] [{idx}] Validating: {title}")
            dp_link = await resolve_to_dp_link(context, page, amz_link, title, author)
            
            new_row = row.copy()
            if dp_link:
                new_row['Amazon Link'] = dp_link
                new_row['Status'] = 'RESOLVED_DP'
                logger.success(f"  [Worker {worker_id}] [RESOLVED] {title}")
            else:
                logger.warning(f"  [Worker {worker_id}] [FAILED] {title}")
                # Keep original or mark specifically
                new_row['Status'] = 'FAILED_DP_RESOLUTION'
            
            results_dict[idx] = new_row
            queue.task_done()
            
            # Atomic save to avoid data loss
            if len(results_dict) % 3 == 0:
                pd.DataFrame.from_dict(results_dict, orient='index').to_csv(VALIDATED_FILE)
            
            await asyncio.sleep(random.uniform(2, 4))
                
    finally:
        await context.close()

async def run_validated_resolution():
    if not os.path.exists(HOTFIX_FILE):
        logger.error(f"Source file {HOTFIX_FILE} missing.")
        return

    # 1. Read the hotfix results
    df = pd.read_csv(HOTFIX_FILE, index_col=0)
    logger.info(f"Loaded {len(df)} entries from {HOTFIX_FILE}")
    
    # 2. Setup Results Dictionary (Picking up from progress if file exists)
    results_dict = {}
    if os.path.exists(VALIDATED_FILE):
        try:
            # We trust RESOLVED_DP status if it has a clean DP link
            old_df = pd.read_csv(VALIDATED_FILE, index_col=0)
            existing_results = old_df.to_dict(orient='index')
            for i, r in existing_results.items():
                amz = str(r.get('Amazon Link', ''))
                if r.get('Status') == 'RESOLVED_DP' and ("/dp/" in amz or "/gp/product/" in amz):
                    results_dict[i] = r
            logger.info(f"Loaded {len(results_dict)} high-confidence DP links from progress.")
        except Exception as e:
            logger.warning(f"Progress clear: {e}")

    # 3. Create Queue
    queue = asyncio.Queue()
    queue_items = []
    for idx, row in df.iterrows():
        if idx not in results_dict:
            queue_items.append((idx, row.to_dict()))
            
    # Randomize to distribute across the sheet
    random.shuffle(queue_items)
    for item in queue_items:
        await queue.put(item)

    if queue.empty():
        logger.info("All links are already resolved to DP.")
        return

    logger.success(f"Starting Phase 12 Resolution with 5 workers. Queue: {queue.qsize()}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 5 Parallel workers for high throughput
        workers = [validation_worker(i, browser, queue, results_dict) for i in range(5)]
        await asyncio.gather(*workers)
        await browser.close()
        
    # 4. Final Save
    final_df = pd.DataFrame.from_dict(results_dict, orient='index')
    final_df.to_csv(VALIDATED_FILE)
    logger.success(f"Validated Resolution Pass Complete. Saved to {VALIDATED_FILE}")

if __name__ == "__main__":
    asyncio.run(run_validated_resolution())
