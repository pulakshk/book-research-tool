import asyncio
import os
import pandas as pd
from loguru import logger
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_GENRE_DIR = os.path.join(SCRIPT_DIR, 'subgenre-pipeline', 'genre-crawl')

import sys
sys.path.insert(0, NEW_GENRE_DIR)
from genre_enrichment import create_stealth_context

async def scrape_bestseller_page(page, url, list_name):
    """Scrape up to 100 results from an Amazon link."""
    logger.info(f"Navigating to {list_name}: {url}")
    try:
        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        await asyncio.sleep(4)
        
        # Determine if Bestseller or Search
        if 'zgbs' in url or 'bestsellers' in url:
            # Bestsellers layout
            els = await page.query_selector_all('div.zg-grid-general-faceout, div.p13n-sc-uncoverable-faceout')
            results = []
            for idx, el in enumerate(els):
                title_el = await el.query_selector("div._cDEzb_p13n-sc-css-line-clamp-1_1Fn1y, span.p13n-sc-truncate-desktop-type2")
                if not title_el:
                    # Alternative selector
                    title_el = await el.query_selector("img")
                
                title = ""
                if title_el:
                    title = await title_el.get_attribute("title") or await title_el.inner_text() or await title_el.get_attribute("alt")
                if title:
                    results.append({'List Name': list_name, 'Rank': idx + 1, 'Book': title.strip()})
            return results
        else:
            # Search layout
            els = await page.query_selector_all('div[data-component-type="s-search-result"]')
            results = []
            for idx, el in enumerate(els):
                title_el = await el.query_selector('h2 a span')
                if title_el:
                    title = await title_el.inner_text()
                    results.append({'List Name': list_name, 'Rank': idx + 1, 'Book': title.strip()})
            return results
    except Exception as e:
        logger.error(f"Failed to scrape {list_name}: {e}")
        return []

async def main():
    csv_path = os.path.join(NEW_GENRE_DIR, 'Az Best sellers_ Crawling - Amazon Links.csv')
    df = pd.read_csv(csv_path)
    
    # Filter out empty links
    links = df[df['Link'].notna() & df['Link'].str.startswith('http')].to_dict(orient='records')
    logger.info(f"Found {len(links)} valid links to scrape.")
    
    all_ranks = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        for item in links:
            list_name = str(item.get('List ', 'Unnamed List')).strip()
            raw_url_string = item['Link']
            
            sub_urls = [u.strip() for u in str(raw_url_string).split('\n') if u.strip().startswith('http')]
            
            for url in sub_urls:
                # Scrape page 1
                results = await scrape_bestseller_page(page, url, list_name)
                all_ranks.extend(results)
                
                # Scrape page 2 if Bestseller layout (to get rank 51-100)
                if 'zgbs' in url or 'bestsellers' in url:
                    try:
                        next_btn = await page.query_selector('li.a-last a')
                        if next_btn:
                            await next_btn.click()
                            await asyncio.sleep(4)
                            results_p2 = await scrape_bestseller_page(page, page.url, list_name)
                            # Fix ranks for page 2
                            for r in results_p2:
                                r['Rank'] += 50
                            all_ranks.extend(results_p2)
                    except Exception as e:
                        logger.warning(f"No page 2 for {list_name}")
                    
        await browser.close()
        
    out_df = pd.DataFrame(all_ranks)
    out_path = os.path.join(NEW_GENRE_DIR, 'Az_Bestsellers_Master_Ranks.csv')
    out_df.to_csv(out_path, index=False)
    logger.success(f"Saved {len(out_df)} BSR records to {out_path}")

if __name__ == '__main__':
    asyncio.run(main())
