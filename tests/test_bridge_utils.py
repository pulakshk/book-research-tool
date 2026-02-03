
import asyncio
import sys
import os
import random
from loguru import logger
from playwright.async_api import async_playwright

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.bridge_utils import extract_amazon_from_goodreads

async def test_bridge():
    test_cases = [
        {"name": "Behind the Net", "gr_url": "https://www.goodreads.com/book/show/174713026-behind-the-net"},
        {"name": "A Lie for a Lie", "gr_url": "https://www.goodreads.com/book/show/44776456-a-lie-for-a-lie"},
        {"name": "Power Plays & Straight A's", "gr_url": "https://www.goodreads.com/book/show/53336266-power-plays-straight-a-s"}
    ]
    
    logger.info(f"Starting Bridge Test for {len(test_cases)} cases...")
    
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Use a real user agent to bypass bot detection in tests
        context = await browser.new_context(user_agent=random.choice(user_agents))
        page = await context.new_page()
        
        results = []
        for case in test_cases:
            logger.info(f"Testing Bridge for: {case['name']}")
            res = await extract_amazon_from_goodreads(page, case['gr_url'])
            if res and res.get('amazon_link'):
                logger.success(f"  ✓ SUCCESS: {res['amazon_link']} (via {res['strategy']})")
                results.append(True)
            else:
                logger.error(f"  ✗ FAILED: Could not bridge {case['name']}")
                results.append(False)
            
            await asyncio.sleep(random.uniform(3, 5))
            
        await browser.close()
        
        success_count = sum(results)
        logger.info(f"Test Finished. Success: {success_count}/{len(test_cases)}")
        
        if success_count == len(test_cases):
            logger.success("✅ ALL TESTS PASSED. Bridge Utility is ready for production integration.")
        else:
            logger.error("❌ TESTS FAILED. Check logs before integrating.")

if __name__ == "__main__":
    asyncio.run(test_bridge())
