#!/usr/bin/env python3
"""
Amazon Extraction Patterns
Robust extraction patterns adapted from pocketfm Amazon_Top_100_Listing.py
Designed to work with Playwright Page objects
"""
import re
from typing import Dict, Optional, List
from playwright.async_api import Page
from bs4 import BeautifulSoup


async def extract_series_info_amazon(page: Page) -> Dict[str, Optional[str]]:
    """
    Extract series name, book number, and total books from Amazon product page
    Uses multiple fallback selectors for robustness
    
    Adapted from Amazon_Top_100_Listing.py lines 224-258
    
    Returns:
        {
            'series_name': str or None,
            'book_number': str or None,
            'total_books': str or None
        }
    """
    result = {
        'series_name': None,
        'book_number': None,
        'total_books': None
    }
    
    try:
        # Strategy 1: RPI icon link (most reliable)
        series_elem = await page.query_selector('#rpi-icon-link-book_details-series')
        if series_elem:
            result['series_name'] = (await series_elem.text_content()).strip()
        
        # Strategy 2: RPI attribute label (Book X of Y pattern)
        book_info_elem = await page.query_selector('#rpi-attribute-book_details-series .rpi-attribute-label span')
        if book_info_elem:
            text = (await book_info_elem.text_content()).strip()
            match = re.match(r'Book\s+(\d+)\s+of\s+(\d+)', text, re.I)
            if match:
                result['book_number'] = match.group(1)
                result['total_books'] = match.group(2)
        
        # Strategy 3: Detail bullets fallback
        if not result['book_number']:
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
                label = li.select_one('.a-text-bold')
                if label and 'Book' in label.text and 'of' in label.text:
                    text = label.text.strip()
                    match = re.match(r'Book\s+(\d+)\s+of\s+(\d+)', text, re.I)
                    if match:
                        result['book_number'] = match.group(1)
                        result['total_books'] = match.group(2)
                    
                    # Also try to get series name from link
                    link = li.select_one('a.a-link-normal span')
                    if link and not result['series_name']:
                        result['series_name'] = link.text.strip()
                    break
    except Exception as e:
        print(f"Error extracting series info: {e}")
    
    return result


async def extract_goodreads_from_amazon(page: Page) -> Dict[str, Optional[str]]:
    """
    Extract Goodreads rating and count directly from Amazon product page
    Saves a separate Goodreads scrape!
    
    Adapted from Amazon_Top_100_Listing.py lines 211-222
    
    Returns:
        {
            'goodreads_rating': str or None,
            'goodreads_rating_count': str or None
        }
    """
    result = {
        'goodreads_rating': None,
        'goodreads_rating_count': None
    }
    
    try:
        # Rating text (e.g., "4.5 on Goodreads")
        rating_elem = await page.query_selector('#reviewFeatureGroup .a-section.gr-review-base .gr-review-rating-text')
        if rating_elem:
            rating_text = (await rating_elem.text_content()).strip()
            result['goodreads_rating'] = rating_text.replace('on Goodreads', '').strip()
        
        # Rating count (e.g., "1,234 ratings")
        count_elem = await page.query_selector('#reviewFeatureGroup .a-section.gr-review-base .gr-review-count-text')
        if count_elem:
            count_text = (await count_elem.text_content()).strip()
            # Remove commas and 'ratings' text
            count_clean = count_text.replace('ratings', '').replace(',', '').strip()
            result['goodreads_rating_count'] = count_clean
    except Exception as e:
        print(f"Error extracting Goodreads data from Amazon: {e}")
    
    return result


async def extract_page_count_amazon(page: Page) -> Optional[str]:
    """
    Extract page count with multiple fallback selectors
    
    Adapted from Amazon_Top_100_Listing.py lines 259-274
    
    Returns:
        Page count as string or None
    """
    try:
        # Strategy 1: RPI attribute
        pages_elem = await page.query_selector('#rpi-attribute-book_details-ebook_pages .rpi-attribute-value span')
        if pages_elem:
            pages_text = (await pages_elem.text_content()).strip()
            # Extract just the number
            match = re.search(r'(\d+)', pages_text)
            if match:
                return match.group(1)
        
        # Strategy 2: Detail bullets
        pages_elem = await page.query_selector('#detail_bullets-book_details-ebook_pages span')
        if pages_elem:
            pages_text = (await pages_elem.text_content()).strip()
            match = re.search(r'(\d+)', pages_text)
            if match:
                return match.group(1)
        
        # Strategy 3: Search in detail bullets list
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        for li in soup.select('ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list li'):
            text = li.get_text(strip=True)
            if 'Print length' in text or 'pages' in text.lower():
                match = re.search(r'(\d+)\s*pages', text, re.I)
                if match:
                    return match.group(1)
    except Exception as e:
        print(f"Error extracting page count: {e}")
    
    return None


async def extract_publisher_amazon(page: Page) -> Optional[str]:
    """
    Extract publisher with multiple strategies
    
    Adapted from Amazon_Top_100_Listing.py lines 185-193
    
    Returns:
        Publisher name or None
    """
    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Strategy 1: Detail bullets with Publisher label
        for li in soup.select('ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list li span.a-list-item'):
            spans = li.select('span')
            if len(spans) >= 2 and 'Publisher' in spans[0].text:
                publisher = spans[1].text.strip()
                # Clean up common patterns like "Publisher (Date)"
                publisher = re.sub(r'\s*\([^)]+\)\s*', '', publisher).strip()
                return publisher
        
        # Strategy 2: Product details table
        for row in soup.select('.a-keyvalue.a-spacing-base tr'):
            th = row.select_one('th')
            if th and 'Publisher' in th.text:
                td = row.select_one('td')
                if td:
                    publisher = td.text.strip()
                    publisher = re.sub(r'\s*\([^)]+\)\s*', '', publisher).strip()
                    return publisher
    except Exception as e:
        print(f"Error extracting publisher: {e}")
    
    return None


async def extract_publication_date_amazon(page: Page) -> Optional[str]:
    """Extract publication date from Amazon detail bullets."""
    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        for li in soup.select('ul.detail-bullet-list li'):
            bold = li.select_one('.a-text-bold')
            if bold and ('Publication date' in bold.text or 'Release date' in bold.text):
                text = li.get_text(strip=True)
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})|([A-Z][a-z]+ \d+, \d{4})', text)
                if date_match: return date_match.group(0)
    except: pass
    return None

async def extract_short_synopsis_amazon(page: Page) -> Optional[str]:
    """Extract first paragraph of description as short synopsis."""
    try:
        desc_elem = await page.query_selector("div[data-feature-name='bookDescription'] .a-expander-content")
        if desc_elem:
            text = (await desc_elem.text_content()).strip()
            paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
            if paragraphs: return paragraphs[0][:300]
    except: pass
    return None

async def extract_best_sellers_rank(page: Page) -> Optional[str]:
    """
    Extract full Best Sellers Rank hierarchy
    Useful for "Featured List" and "Top Lists" fields
    
    Adapted from Amazon_Top_100_Listing.py lines 275-297
    
    Returns:
        Pipe-separated rank string (e.g., "#88 Free in Kindle Store | #1 in Sports Romance")
    """
    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for Best Sellers Rank in detail bullets
        for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
            bold = li.select_one('.a-text-bold')
            if not bold or 'Best Sellers Rank' not in bold.text:
                continue
            
            parts = []
            
            # Check for nested zg_hrsr list (category hierarchy)
            sub_ul = li.select_one('ul.zg_hrsr')
            if sub_ul:
                for sub_li in sub_ul.select('li span.a-list-item'):
                    rank_text = sub_li.get_text(strip=True)
                    if rank_text:
                        parts.append(rank_text)
            else:
                # Single line rank
                rest = li.get_text(separator=' ', strip=True)
                idx = rest.find('Best Sellers Rank')
                if idx >= 0:
                    rank_text = rest[idx:].replace('Best Sellers Rank :', '').replace('Best Sellers Rank:', '').strip()
                    # Remove "See Top 100" links
                    rank_text = re.sub(r'\(See Top \d+.*?\)', '', rank_text).strip()
                    if rank_text:
                        parts.append(rank_text)
            
            if parts:
                return ' | '.join(parts)
        
        # Fallback: Check product details table
        for row in soup.select('.a-keyvalue.a-vertical-stripes.a-span6 tr'):
            th = row.select_one('th')
            if th and 'Rank' in th.text.strip():
                tds = row.select('td span')
                rank_text = ' | '.join([t.text.strip() for t in tds if t.text.strip()])
                if rank_text:
                    return rank_text
    except Exception as e:
        print(f"Error extracting best sellers rank: {e}")
    
    return None


async def scroll_for_dynamic_content(page: Page, max_scrolls: int = 15, delay_ms: int = 400) -> None:
    """
    Scroll page incrementally to load lazy-loaded content
    Useful for reviews, product details, etc.
    
    Adapted from Amazon_Top_100_Listing.py lines 360-371
    
    Args:
        page: Playwright Page object
        max_scrolls: Maximum number of scroll iterations
        delay_ms: Delay between scrolls in milliseconds
    """
    try:
        previous_height = 0
        stable_count = 0
        
        for i in range(max_scrolls):
            # Get current page height
            current_height = await page.evaluate('document.body.scrollHeight')
            
            # Scroll by one window height
            await page.evaluate('window.scrollBy(0, window.innerHeight)')
            await page.wait_for_timeout(delay_ms)
            
            # Check if height stabilized
            if current_height == previous_height:
                stable_count += 1
                if stable_count >= 3:
                    # Height hasn't changed for 3 iterations, we're done
                    break
            else:
                stable_count = 0
            
            previous_height = current_height
    except Exception as e:
        print(f"Error during scroll: {e}")


async def extract_amazon_comprehensive(page: Page, scroll_first: bool = True) -> Dict[str, Optional[str]]:
    """
    Extract all available Amazon data using combined patterns
    
    Args:
        page: Playwright Page object (must already be on Amazon product page)
        scroll_first: Whether to scroll page first to load dynamic content
    
    Returns:
        Dict with all extracted fields
    """
    # Extract all components
    series_info = await extract_series_info_amazon(page)
    goodreads_info = await extract_goodreads_from_amazon(page)
    pages = await extract_page_count_amazon(page)
    publisher = await extract_publisher_amazon(page)
    rank = await extract_best_sellers_rank(page)
    pub_date = await extract_publication_date_amazon(page)
    short_syn = await extract_short_synopsis_amazon(page)
    
    # Get basic fields (title, author, rating)
    title = None
    author = None
    amazon_rating = None
    amazon_rating_count = None
    
    try:
        title_elem = await page.query_selector('#productTitle')
        if title_elem:
            title = (await title_elem.text_content()).strip()
    except:
        pass
    
    try:
        author_elem = await page.query_selector('.author .a-link-normal')
        if author_elem:
            author = (await author_elem.text_content()).strip()
    except:
        pass
    
    try:
        rating_elem = await page.query_selector('.reviewCountTextLinkedHistogram')
        if rating_elem:
            rating_title = await rating_elem.get_attribute('title')
            if rating_title:
                amazon_rating = rating_title.split(' ')[0]
    except:
        pass
    
    try:
        count_elem = await page.query_selector('#acrCustomerReviewText')
        if count_elem:
            count_text = (await count_elem.text_content()).strip()
            # Remove commas: "25,410 ratings" -> "25410"
            amazon_rating_count = re.sub(r'[^\d]', '', count_text)
    except:
        pass
    
    return {
        'title': title,
        'author': author,
        'amazon_rating': amazon_rating,
        'amazon_rating_count': amazon_rating_count,
        'series_name': series_info['series_name'],
        'book_number': series_info['book_number'],
        'total_books_in_series': series_info['total_books'],
        'pages': pages,
        'publisher': publisher,
        'best_sellers_rank': rank,
        'goodreads_rating': goodreads_info['goodreads_rating'],
        'goodreads_rating_count': goodreads_info['goodreads_rating_count'],
        'publication_date': pub_date,
        'short_synopsis': short_syn
    }


# Convenience function for testing
async def test_amazon_extraction(url: str):
    """Test extraction on a specific Amazon URL"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        await page.goto(url, wait_until='domcontentloaded')
        await page.wait_for_timeout(2000)
        
        data = await extract_amazon_comprehensive(page)
        
        print("Extracted Amazon Data:")
        print("=" * 80)
        for key, value in data.items():
            print(f"{key:30s}: {value}")
        
        await browser.close()


if __name__ == "__main__":
    import asyncio
    
    # Test with a sample URL
    test_url = "https://www.amazon.com/dp/B0B7JH11C1"  # Example book
    asyncio.run(test_amazon_extraction(test_url))
