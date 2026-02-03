
import asyncio
from playwright.async_api import async_playwright

async def debug_search():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        query = "The Fake Out Vancouver Storm"
        url = f"https://www.amazon.com/s?k={query.replace(' ', '+')}"
        print(f"Searching: {url}")
        
        await page.goto(url)
        await asyncio.sleep(3)
        
        # Check title
        page_title = await page.title()
        print(f"Page Title: {page_title}")
        
        # Check results
        results = await page.query_selector_all("div[data-component-type='s-search-result']")
        print(f"Found {len(results)} search results.")
        
        if results:
            target = results[0]
            # Try to get author
            # Looking for common author patterns
            text = await target.inner_text()
            print("--- Result 0 Content ---")
            print(text[:500])
            
            author_els = await target.query_selector_all(".a-color-secondary .a-size-base, span.a-size-base")
            print(f"Found {len(author_els)} possible author elements.")
            for el in author_els:
                t = await el.inner_text()
                print(f"  - {t}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_search())
