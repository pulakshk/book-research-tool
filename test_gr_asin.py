import asyncio
import os
import sys
from playwright.async_api import async_playwright
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, 'New genre crawl'))
from genre_enrichment import create_stealth_context

async def test():
    asin = 'B01G7X9UBI'
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        url = f"https://www.goodreads.com/book/isbn/{asin}"
        print(f"Loading {url}")
        await page.goto(url)
        await asyncio.sleep(4)
        
        print(f"Current URL: {page.url}")
        
        title_el = await page.query_selector("h1[data-testid='bookTitle']")
        if title_el:
            title = await title_el.text_content()
            print(f"Found book: {title}")
        else:
            print("Title not found.")
            content = await page.content()
            if "captcha" in content.lower():
                print("Hit CAPTCHA!")
            else:
                print("HTML snippet:", content[:500])
                
        await page.close()
        await context.close()
        await browser.close()

if __name__ == '__main__':
    asyncio.run(test())
