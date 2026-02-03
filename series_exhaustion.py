
import asyncio
import pandas as pd
import numpy as np
import os
import re
import random
from loguru import logger
from playwright.async_api import async_playwright
import sys
# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.filter import is_sports_hockey_related

# Config
INPUT_FILE = "base_for_parallel.csv"
OUTPUT_FILE = "series_exhausted.csv"

class CaptchaDetected(Exception): pass

async def get_new_context(browser):
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ]
    context = await browser.new_context(user_agent=random.choice(user_agents))
    page = await context.new_page()
    return context, page

async def get_series_data(page, series_url):
    """Scrape all books and metadata from a Goodreads series page."""
    try:
        await page.goto(series_url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(3)
        
        meta = {'total_books': 0, 'status': 'Unknown'}
        header_text = await page.inner_text("div.seriesHeader, h1")
        if header_text:
            count_match = re.search(r'(\d+)\s+books', header_text, re.IGNORECASE)
            if count_match: meta['total_books'] = int(count_match.group(1))
            if "finished" in header_text.lower() or "complete" in header_text.lower():
                meta['status'] = "Completed"
            else: meta['status'] = "Ongoing"

        books = []
        items = await page.query_selector_all("div.listWithDivider__item, div[itemprop='itemListElement'], .elementList")
        
        for item in items:
            title_el = await item.query_selector("a.bookTitle, a[itemprop='url'] span[itemprop='name']")
            if not title_el: continue
            title = (await title_el.inner_text()).strip()
            
            href = await title_el.get_attribute("href")
            link = href if href.startswith('http') else "https://www.goodreads.com" + href

            author_el = await item.query_selector("span[itemprop='author'] span[itemprop='name'], a.authorName")
            author = (await author_el.inner_text()).strip() if author_el else ""
            
            series_info_el = await item.query_selector("h3, .gr-h3--noMargin, .bookSeries")
            series_info = (await series_info_el.inner_text()).strip() if series_info_el else ""
            
            stats_text = await item.inner_text()
            rating, rating_count = 0.0, 0
            r_match = re.search(r'([\d.]+)\s*[··]\s*([\d,]+)\s*Ratings?', stats_text)
            if r_match:
                rating = float(r_match.group(1))
                rating_count = int(r_match.group(2).replace(',', ''))
            
            books.append({
                'title': title, 'link': link, 'author': author,
                'rating': rating, 'rating_count': rating_count,
                'series_info': series_info, 'total_books': meta['total_books'],
                'status': meta['status']
            })
        return books
    except Exception as e:
        logger.error(f"Error scraping {series_url}: {e}")
        return []

async def exhaust_series():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"File {INPUT_FILE} not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    # Target series that have a Goodreads link or we can find one
    # For exhaustion, we only care about series names we have
    unique_series = df[df['Series Name'].notna()]['Series Name'].unique()
    logger.info(f"Starting Exhaustion Phase for {len(unique_series)} series.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context, page = await get_new_context(browser)

        new_books_injected = 0
        count = 0
        
        for series_name in unique_series:
            # Clean name: "Beyond the Play (#2)" -> "Beyond the Play"
            clean_name = re.sub(r'\s*\(#\d+\)', '', str(series_name)).strip()
            clean_name = re.sub(r'[,#\s]+$', '', clean_name).strip()
            
            if not clean_name or clean_name.lower() == 'nan': continue
            
            logger.info(f"Checking exhaustion for: {series_name} (Clean: {clean_name})")
            
            # RELEVANCE CHECK: Skip if not sports/hockey related
            if not is_sports_hockey_related(clean_name):
                logger.info(f"Skipping non-sports series: {clean_name}")
                continue
            
            # Rotate context every 10 series
            if count > 0 and count % 10 == 0:
                await context.close()
                context, page = await get_new_context(browser)

            try:
                # OPTIMIZATION: Check if we have an existing GR link for any book in this series
                series_rows = df[df['Series Name'] == series_name]
                existing_glink = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None
                
                success = False
                for attempt in range(2): # Try 2 times per series
                    try:
                        if existing_glink:
                            logger.info(f"  -> Using existing link to find series (Attempt {attempt+1}): {existing_glink}")
                            await page.goto(existing_glink, wait_until="domcontentloaded", timeout=45000)
                        else:
                            # Search if no link found
                            search_url = f"https://www.goodreads.com/search?q={clean_name.replace(' ', '+')}&search_type=books"
                            logger.info(f"  -> Searching GR (Attempt {attempt+1}): {search_url}")
                            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                            
                            # Captcha Check
                            content = await page.content()
                            if "captcha" in content.lower() or "robot check" in content.lower():
                                raise CaptchaDetected("Goodreads Captcha")

                            try:
                                await page.wait_for_selector("a.bookTitle", timeout=15000)
                            except:
                                if attempt == 0: 
                                    logger.warning(f"  !! Timeout on search, retrying...")
                                    await asyncio.sleep(5)
                                    continue
                                else: raise Exception("Search timeout after retries")
                                
                            first_book = await page.query_selector("a.bookTitle")
                            if not first_book: 
                                logger.warning(f"  !! No book results for {clean_name}")
                                break
                            await first_book.click()
                            await page.wait_for_load_state("domcontentloaded", timeout=60000)
                        
                        # Resiliently find the series link on the book page
                        series_link_selector = "a[aria-label*='series'], .BookPageTitleSection__series a, a[href*='/series/'], [data-testid='series'] a"
                        try:
                            await page.wait_for_selector(series_link_selector, timeout=10000)
                        except:
                            pass
                            
                        series_link_el = await page.query_selector(series_link_selector)
                        series_url = None
                        if series_link_el:
                            href = await series_link_el.get_attribute("href")
                            series_url = href if href.startswith('http') else "https://www.goodreads.com" + href
                        
                        if not series_url:
                            # FALLBACK: Try search for series directly if book page failed
                            logger.info(f"  -> Book page failed to show series. Trying direct series search for '{clean_name}'")
                            s_search_url = f"https://www.goodreads.com/search?q={clean_name.replace(' ', '+')}&search_type=series"
                            if await safe_goto(page, s_search_url):
                                s_link_el = await page.query_selector("a[href*='/series/']")
                                if s_link_el:
                                    href = await s_link_el.get_attribute("href")
                                    series_url = href if href.startswith('http') else "https://www.goodreads.com" + href
                        
                        if not series_url:
                            logger.warning(f"  -> Series link not found for {clean_name}")
                            break
                        
                        # 2. Scrape all books in series
                        series_books = await get_series_data(page, series_url)
                        logger.info(f"  -> Series has {len(series_books)} books on GR.")
                        
                        if not series_books:
                            logger.warning(f"  -> Scraped 0 books for {series_url}")
                            break
                        
                        success = True
                        break # Success!
                    except CaptchaDetected:
                        logger.error("CAPTCHA detected! Rotating context and waiting...")
                        await context.close()
                        await asyncio.sleep(random.uniform(5, 10))
                        context, page = await get_new_context(browser)
                    except Exception as e:
                        logger.warning(f"Attempt {attempt+1} failed: {e}")
                        await asyncio.sleep(2)

                if not success:
                    logger.error(f"Failed to process series: {series_name} after {attempt+1} attempts.")
                    continue

                # 3. Compare and Inject

                # 3. Compare and Inject
                for b in series_books:
                    norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                    norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                    
                    # Check if exists (with some fuzziness)
                    # We check for exact match or if title is contained
                    mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                           (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                    
                    exists = df[mask]
                    
                    if exists.empty:
                        # RELEVANCE CHECK: Only inject if book itself is sports/hockey related
                        # We check title and series name (already checked series name, but good for title)
                        if not is_sports_hockey_related(b['title'], {'series': series_name}):
                            logger.info(f"  - Skipping non-sports book in series: {b['title']}")
                            continue
                            
                        logger.success(f"  + Injecting missing book: {b['title']} by {b['author']}")
                        # Initialize new row with NaNs
                        new_row = {col: np.nan for col in df.columns}
                        new_row.update({
                            'Series Name': series_name,
                            'Author Name': b['author'],
                            'Book Name': b['title'],
                            'Goodreads Link': b['link'],
                            'Goodreads Rating': b['rating'],
                            'Goodreads # of Ratings': b['rating_count'],
                            'Total Books in Series': b['total_books'],
                            'Series Status': b['status'],
                            'Primary Subgenre': 'Hockey Romance' # Baseline
                        })
                        # Extract book number
                        num_match = re.search(r'Book\s*(\d+)', b['series_info'])
                        if num_match: 
                            new_row['Book Number'] = float(num_match.group(1))
                        
                        # Use concat instead of append
                        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                        new_books_injected += 1
                    else:
                        # Optionally update existing row if it's missing the GR link or series info
                        idx = exists.index[0]
                        if pd.isna(df.at[idx, 'Goodreads Link']): df.at[idx, 'Goodreads Link'] = b['link']
                        if pd.isna(df.at[idx, 'Total Books in Series']): df.at[idx, 'Total Books in Series'] = b['total_books']
                
                # Save progress
                df.to_csv(OUTPUT_FILE, index=False)
                
            except Exception as e:
                logger.error(f"Error exhausting {series_name}: {e}")
            
            await asyncio.sleep(random.uniform(3, 6))

        await browser.close()
        logger.success(f"Series Exhaustion Complete. Injected {new_books_injected} new books.")

if __name__ == "__main__":
    asyncio.run(exhaust_series())
