
import asyncio
import random
from loguru import logger
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

async def safe_goto(page, url, timeout=45000, retries=3):
    """Shared network-resilient navigation."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            return True
        except Exception as e:
            logger.debug(f"      - goto attempt {attempt}/{retries} failed: {e}")
            await asyncio.sleep(2 * attempt + random.uniform(0.5, 1.5))
    return False

async def extract_amazon_from_goodreads(page, gr_url):
    """
    Navigates to Goodreads and extracts the Amazon link from the 'Get a Copy' page.
    This bridge strategy has a high success rate for books suppressed by Amazon search.
    """
    try:
        # Normalize URL to get_a_copy subpage
        base_url = gr_url.split('?')[0].replace('/show/', '/').rstrip('/')
        if not base_url.endswith("/get_a_copy"):
            bridge_url = base_url + "/get_a_copy"
        else:
            bridge_url = base_url
            
        logger.debug(f"    - Bridging GR -> Amazon via: {bridge_url}")
        
        if not await safe_goto(page, bridge_url): 
            return None
            
        # Wait for any store link to appear
        try:
            await page.wait_for_selector("a[href*='book_link/follow/']", timeout=15000)
        except:
            logger.debug("      - Timeout waiting for store links on GR")
        
        # 1. Primary: Look for Amazon provider link (ID 1 is usually Amazon)
        amz_selectors = [
            "a[href*='book_link/follow/1']",
            "a.actionLinkLite[href*='book_link/follow/1']",
            "a.buyButton[href*='book_link/follow/1']"
        ]
        
        for selector in amz_selectors:
            amz_redirect = await page.query_selector(selector)
            if amz_redirect:
                href = await amz_redirect.get_attribute("href")
                meta_url = href if href.startswith("http") else "https://www.goodreads.com" + href
                
                # NEW: Follow the redirect to get the actual Amazon landing page
                logger.debug(f"      - Following GR-Amazon Redirect: {meta_url}")
                if await safe_goto(page, meta_url):
                    final_url = page.url
                    # Strip tracking if it's a direct product page
                    if "/dp/" in final_url or "/gp/product/" in final_url:
                        final_url = final_url.split("?")[0]
                        logger.debug(f"      - Resolved to Direct Amazon DP: {final_url}")
                        return {'amazon_link': final_url, 'strategy': 'Bridge_Redirect'}
                    elif any(pat in final_url for pat in ["/s?k=", "s-k=", "keywords=", "/s/ref="]):
                        logger.warning(f"      ⚠ Landed on SEARCH page: {final_url}")
                        # NEW: Parse the search page for the first direct link (using broader selectors)
                        selectors = [
                            "div[data-component-type='s-search-result'] h2 a.a-link-normal",
                            "h2 a.a-link-normal",
                            "a.a-link-normal .a-size-medium",
                            "a.a-link-normal .a-size-base-plus"
                        ]
                        for sel in selectors:
                            first_item = await page.query_selector(sel)
                            if first_item:
                                # If it's the span inside the link, get the parent
                                if await first_item.get_attribute("href") is None:
                                    # Check if the parent is the link
                                    parent = await first_item.query_selector("xpath=..")
                                    if parent and await parent.get_attribute("href"):
                                        first_item = parent
                                
                                href = await first_item.get_attribute("href")
                                if href and "/dp/" in href:
                                    dp_link = "https://www.amazon.com" + href.split("?")[0]
                                    logger.success(f"      ✓ Extracted DP from search landing: {dp_link}")
                                    return {'amazon_link': dp_link, 'strategy': 'Bridge_Search_Extraction'}
                
                return {'amazon_link': meta_url, 'strategy': 'Bridge_ID1'}

        # 2. Secondary: Look for any link with "Amazon" in text on this page
        all_links = await page.query_selector_all("a")
        for link in all_links:
            text = (await link.inner_text()).lower()
            if "amazon" in text:
                href = await link.get_attribute("href")
                if href:
                    res_url = href if href.startswith("http") else "https://www.goodreads.com" + href
                    logger.debug(f"      - Found Amazon Link via text matching: {res_url}")
                    return {'amazon_link': res_url, 'strategy': 'Bridge_Text'}
                    
        return None
    except Exception as e:
        logger.warning(f"      - GR -> Amazon bridge error: {e}")
        return None
