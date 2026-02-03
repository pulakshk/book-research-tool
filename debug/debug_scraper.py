
import asyncio
from playwright.async_api import async_playwright

URLS = [
    "https://www.amazon.com/dp/B0CCPRQDZT",
    "https://www.amazon.com/dp/B0CSXF1WDP"
]

async def debug_scrape():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
             user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        for url in URLS:
            print(f"Checking {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)
                
                title = await page.title()
                print(f"Title: {title}")
                
                content = await page.content()
                if "captcha" in content.lower() or "robot check" in title.lower():
                    print("BLOCKED BY CAPTCHA")
                else:
                    # Check for selectors
                    links = await page.query_selector_all("a[href*='/gp/bestsellers/'], a[href*='/zgbs/']")
                    print(f"Found {len(links)} bestseller links")
                    
                    # Dump html for inspection
                    with open(f"debug_{url.split('/')[-1]}.html", "w") as f:
                        f.write(content)
                        
            except Exception as e:
                print(f"Error: {e}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_scrape())
