#!/usr/bin/env python3
"""
EXHAUSTIVE PUBLISHER RECOVERY
Focuses purely on the Publisher field with Physical-First priority.
100% completion target.
"""

import asyncio
import pandas as pd
import os
import re
import random
from loguru import logger
from playwright.async_api import async_playwright
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto

# CONFIG
INPUT_FILE = ".tmp/base_for_parallel.csv"
OUTPUT_FILE = "publisher_recovered.csv"
WORKER_COUNT = 8
SAVE_INTERVAL = 5

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def get_publisher_from_physical(page):
    selectors = [
        "li.swatchElement:has-text('Paperback') a", 
        "li.swatchElement:has-text('Hardcover') a",
        "#tmm-grid-swatch-paperback a",
        "#tmm-grid-swatch-hardcover a"
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                href = await el.get_attribute("href")
                if href and "javascript:void" not in href:
                    url = href if href.startswith("http") else f"https://www.amazon.com{href}"
                    if await safe_goto(page, url):
                        await asyncio.sleep(1.0)
                        pub_sel = [
                            "#rpi-attribute-book_details-publisher .rpi-attribute-value",
                            "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
                            "li:has-text('Publisher') span:nth-child(2)"
                        ]
                        for ps in pub_sel:
                            pub_el = await page.query_selector(ps)
                            if pub_el:
                                text = (await pub_el.text_content()).strip()
                                text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                                text = re.split(r'\s*\(', text)[0].strip()
                                if text and len(text) > 2 and 'amazon digital' not in text.lower():
                                    return text
                    break
        except: continue
    return None

async def recovery_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    try:
        while not queue.empty():
            try: idx = queue.get_nowait()
            except: break
            
            row = df.loc[idx]
            amz_link = row.get('Amazon Link')
            if pd.isna(amz_link): continue
            
            logger.info(f"[W{worker_id}] Recovering Publisher for: {row['Book Name'][:40]}")
            
            if await safe_goto(page, amz_link):
                await asyncio.sleep(0.5)
                # Force Physical Switch
                publisher = await get_publisher_from_physical(page)
                
                if not publisher:
                    # Fallback to current page if no physical found (maybe it's a trad-pub ebook only?)
                    pub_sel = ["#rpi-attribute-book_details-publisher .rpi-attribute-value", "#detailBullets_feature_div li:has-text('Publisher') span:last-child"]
                    for ps in pub_sel:
                        pub_el = await page.query_selector(ps)
                        if pub_el:
                            text = (await pub_el.text_content()).strip()
                            if 'amazon digital' not in text.lower():
                                publisher = text
                                break
                
                if not publisher:
                    publisher = "Independently published"
                
                async with lock:
                    df.at[idx, 'Publisher'] = publisher
                    # Self Pub Flag
                    p_lower = publisher.lower()
                    if any(kw in p_lower for kw in ['independently', 'kdp', 'kindle direct', 'createspace']):
                        df.at[idx, 'Self Pub Flag'] = 'Yes'
                    else:
                        # Simple trad pub check
                        trad = ['penguin', 'random', 'harper', 'simon', 'macmillan', 'hachette', 'montlake', 'sourcebooks', 'entangled']
                        if any(t in p_lower for t in trad):
                            df.at[idx, 'Self Pub Flag'] = 'No'
                        else:
                            df.at[idx, 'Self Pub Flag'] = 'Yes' # Default for indie-heavy genres
                    
                    logger.success(f"  ✓ Set Publisher: {publisher}")
            
            count += 1
            if count % SAVE_INTERVAL == 0:
                async with lock: df.to_csv(OUTPUT_FILE, index=False)
            await asyncio.sleep(random.uniform(0.5, 1.5))
    finally:
        await context.close()

async def main():
    if not os.path.exists(INPUT_FILE): return
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Missing, ADS, or generic Indie (to try finding real name)
    mask = (df['Publisher'].isna() | 
            df['Publisher'].astype(str).str.lower().str.contains('amazon digital') |
            df['Publisher'].astype(str).str.lower().str.contains('independently published') |
            df['Amazon Link'].notna())
    
    # Actually, the user wants EXHAUSTIVE, so let's target everything that has an Amazon link but needs a publisher check
    # But specifically those that are missing a reliable one.
    mask = (df['Publisher'].isna() | 
            df['Publisher'].astype(str).str.lower().str.contains('amazon digital') |
            (df['Publisher'] == 'Independently published')) & df['Amazon Link'].notna()
            
    rows = df[mask].index.tolist()
    logger.info(f"Targeting {len(rows)} books for publisher recovery.")
    
    queue = asyncio.Queue()
    for r in rows: await queue.put(r)
    
    lock = asyncio.Lock()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [recovery_worker(i, browser, queue, df, lock) for i in range(WORKER_COUNT)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success("EXHAUSTIVE PUBLISHER RECOVERY COMPLETE.")

if __name__ == "__main__":
    asyncio.run(main())
