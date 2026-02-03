#!/usr/bin/env python3
"""
Goodreads Extraction Patterns
Robust extraction patterns adapted from pocketfm goodread_book_extra_crawl_copy.py
Designed to work with Playwright Page objects
"""
import re
import json
from typing import Dict, Optional, List
from playwright.async_api import Page
from bs4 import BeautifulSoup


async def extract_json_ld_data(page: Page) -> Dict:
    """
    Extract JSON-LD structured data from Goodreads book page
    This is the MOST RELIABLE data source - use as priority #1
    
    Adapted from goodread_book_extra_crawl_copy.py lines 173-177
    
    Returns:
        Parsed JSON-LD dict or empty dict if not found
        Common fields: numberOfPages, ratingValue, ratingCount, author, name, etc.
    """
    try:
        json_ld_elem = await page.query_selector('script[type="application/ld+json"]')
        if json_ld_elem:
            json_text = await json_ld_elem.text_content()
            json_data = json.loads(json_text)
            return json_data
    except Exception as e:
        print(f"Error extracting JSON-LD: {e}")
    
    return {}


async def extract_genres(page: Page, max_genres: int = 5) -> List[str]:
    """
    Extract top genres from Goodreads book page
    Useful for "Primary Trope" field
    
    Adapted from goodread_book_extra_crawl_copy.py lines 217-222
    
    Args:
        page: Playwright Page object
        max_genres: Maximum number of genres to return
    
    Returns:
        List of genre strings
    """
    genres = []
    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Selector: .BookPageMetadataSection__genres .Button__labelItem
        genre_elems = soup.select('.BookPageMetadataSection__genres .Button__labelItem')
        
        for elem in genre_elems[:max_genres]:
            genre = elem.text.strip()
            if genre:
                genres.append(genre)
    except Exception as e:
        print(f"Error extracting genres: {e}")
    
    return genres


async def extract_publication_info(page: Page) -> Dict[str, Optional[str]]:
    """
    Extract publication information (date, publisher, etc.)
    
    Adapted from goodread_book_extra_crawl_copy.py lines 223-226
    Extended with "Show more" expansion logic as per user request.
    
    Returns:
        {
            'publication_date': str or None,
            'full_pub_text': str or None,
            'publisher': str or None,
            'original_publication_date': str or None
        }
    """
    result = {
        'publication_date': None,
        'full_pub_text': None,
        'publisher': None,
        'original_publication_date': None
    }
    
    try:
        # Strategy 1: Primary publicationInfo line
        pub_elem = await page.query_selector('p[data-testid="publicationInfo"]')
        if pub_elem:
            pub_text = (await pub_elem.text_content()).strip()
            result['full_pub_text'] = pub_text
            
            # Format: "First published September 27, 2022 by [Publisher Name]"
            match = re.search(r'(?i)published\s+(.*?)(?:\s+by\s+(.*))?$', pub_text)
            if match:
                result['publication_date'] = match.group(1).strip()
                if match.group(2):
                    result['publisher'] = match.group(2).strip()
        
        # Strategy 2: Expand "Book details & editions" for deeper data
        try:
            show_more = await page.query_selector('button:has-text("Book details & editions"), .BookPageMetadataSection__expandButton')
            if show_more:
                await show_more.scroll_into_view_if_needed()
                await show_more.click()
                await asyncio.sleep(0.5)
                # Small scroll to trigger any lazy loading as suggested by user
                await page.mouse.wheel(0, 500)
                await asyncio.sleep(0.3)
                
                # Expand specific detail items if they have their own "...more"
                more_details = await page.query_selector_all('.DescListItem button:has-text("...more")')
                for btn in more_details:
                    await btn.click()
                
                # Extract Publisher from expanded list
                details_pub = await page.query_selector('dt:has-text("Publisher") + dd, .DescListItem:has-text("Publisher") .DescListItem__content')
                if details_pub:
                    result['publisher'] = (await details_pub.text_content()).strip()
                
                # Extract Original Publication Date
                original_date = await page.query_selector('dt:has-text("Original publication date") + dd, .DescListItem:has-text("Original publication date") .DescListItem__content')
                if original_date:
                    result['original_publication_date'] = (await original_date.text_content()).strip()
        except:
            pass
                
    except Exception as e:
        print(f"Error extracting publication info: {e}")
    
    return result


async def extract_detail_list_items(page: Page) -> Dict[str, str]:
    """
    Extract ISBN, ASIN, Settings, Awards, Publisher from detail list
    These appear as key-value pairs in the book details section
    
    Adapted from goodread_book_extra_crawl_copy.py lines 255-273
    
    Returns:
        Dict with extracted fields
    """
    details = {
        'isbn': None,
        'asin': None,
        'settings': None,
        'awards': None,
        'publisher': None,
        'original_title': None,
        'literary_awards': None
    }
    
    try:
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        # Look for .CollapsableList .DescListItem or similar detail list patterns
        detail_items = soup.select('.CollapsableList .DescListItem')
        
        if not detail_items:
            # Fallback: Try other common selectors
            detail_items = soup.select('.BookDetails .DescListItem')
        
        if not detail_items:
            # Another fallback: data-testid patterns
            detail_items = soup.select('[data-testid*="details"] dt')
        
        for item in detail_items:
            try:
                # Get label and value
                label_elem = item.select_one('dt, .DescListItem__label')
                value_elem = item.select_one('dd, .DescListItem__content')
                
                if not label_elem:
                    label_elem = item
                if not value_elem:
                    # Value might be in next sibling
                    value_elem = item.find_next_sibling()
                
                if label_elem and value_elem:
                    label = label_elem.text.strip().lower()
                    value = value_elem.text.strip()
                    
                    if 'isbn' in label and not 'isbn13' in label:
                        details['isbn'] = value
                    elif 'asin' in label:
                        details['asin'] = value
                    elif 'setting' in label:
                        details['settings'] = value
                    elif 'award' in label:
                        if details['awards']:
                            details['awards'] += f", {value}"
                        else:
                            details['awards'] = value
                    elif 'publisher' in label:
                        details['publisher'] = value
                    elif 'original title' in label:
                        details['original_title'] = value
                    elif 'literary award' in label:
                        details['literary_awards'] = value
            except Exception as e:
                continue
    except Exception as e:
        print(f"Error extracting detail list items: {e}")
    
    return details


async def extract_page_count_goodreads(page: Page) -> Optional[str]:
    """
    Extract page count from Goodreads
    Multiple strategies for robustness
    
    Returns:
        Page count as string or None
    """
    try:
        # Strategy 1: JSON-LD (most reliable)
        json_data = await extract_json_ld_data(page)
        if json_data.get('numberOfPages'):
            return str(json_data['numberOfPages'])
        
        # Strategy 2: pagesFormat test id
        pages_elem = await page.query_selector('p[data-testid="pagesFormat"]')
        if pages_elem:
            pages_text = (await pages_elem.text_content()).strip()
            match = re.search(r'(\d+)\s*pages', pages_text, re.I)
            if match:
                return match.group(1)
        
        # Strategy 3: Search in detail items
        details = await extract_detail_list_items(page)
        for key, value in details.items():
            if value and 'pages' in str(value).lower():
                match = re.search(r'(\d+)\s*pages', str(value), re.I)
                if match:
                    return match.group(1)
    except Exception as e:
        print(f"Error extracting page count from Goodreads: {e}")
    
    return None


async def extract_description_goodreads(page: Page) -> Optional[str]:
    """
    Extract book description/summary from Goodreads
    
    Returns:
        Description text or None
    """
    try:
        # Expand description if "...more" link exists
        try:
            more_link = await page.query_selector('[data-testid="description"] .Button--link, .truncatedContent .moreLink')
            if more_link:
                await more_link.click()
                await asyncio.sleep(0.3)
        except:
            pass

        # Try multiple selectors
        selectors = [
            '[data-testid="description"] .Formatted',
            '.DetailsLayoutRightParagraph .Formatted',
            '.BookPageMetadataSection__description .Formatted',
            'div[data-testid="description"]'
        ]
        
        for selector in selectors:
            desc_elem = await page.query_selector(selector)
            if desc_elem:
                desc_text = (await desc_elem.text_content()).strip()
                if desc_text and len(desc_text) > 50:  # Ensure it's substantial
                    return desc_text
        
        # Fallback: Try to find any large text block
        html = await page.content()
        soup = BeautifulSoup(html, 'html.parser')
        
        for div in soup.select('div.Formatted'):
            text = div.text.strip()
            if len(text) > 100:
                return text
    except Exception as e:
        print(f"Error extracting description: {e}")
    
    return None


async def extract_rating_info_goodreads(page: Page) -> Dict[str, Optional[str]]:
    """
    Extract rating and rating count from Goodreads
    
    Returns:
        {
            'rating': str or None,
            'rating_count': str or None,
            'reviews_count': str or None
        }
    """
    result = {
        'rating': None,
        'rating_count': None,
        'reviews_count': None
    }
    
    try:
        # Strategy 1: JSON-LD
        json_data = await extract_json_ld_data(page)
        if json_data.get('aggregateRating'):
            agg_rating = json_data['aggregateRating']
            result['rating'] = str(agg_rating.get('ratingValue', ''))
            result['rating_count'] = str(agg_rating.get('ratingCount', ''))
            result['reviews_count'] = str(agg_rating.get('reviewCount', ''))
        
        # Strategy 2: Page selectors (backup)
        if not result['rating']:
            rating_elem = await page.query_selector('[data-testid="ratingStatistics"] .RatingStatistics__rating')
            if rating_elem:
                result['rating'] = (await rating_elem.text_content()).strip()
        
        if not result['rating_count']:
            count_elem = await page.query_selector('[data-testid="ratingsCount"]')
            if count_elem:
                count_text = (await count_elem.text_content()).strip()
                # Remove commas and non-digits
                result['rating_count'] = re.sub(r'[^\d]', '', count_text)
        
        if not result['reviews_count']:
            reviews_elem = await page.query_selector('[data-testid="reviewsCount"]')
            if reviews_elem:
                reviews_text = (await reviews_elem.text_content()).strip()
                result['reviews_count'] = re.sub(r'[^\d]', '', reviews_text)
    except Exception as e:
        print(f"Error extracting rating info: {e}")
    
    return result


async def extract_store_links(page: Page) -> Dict[str, str]:
    """
    Extract store links (Amazon, Kindle, etc.) from Goodreads book page
    
    Returns:
        Dict mapping store name to URL
    """
    links = {}
    try:
        # Check primary "Get a copy" buttons
        buy_buttons = await page.query_selector_all('.BookPageMetadataSection__buyButtons a')
        for btn in buy_buttons:
            text = (await btn.text_content() or "").lower()
            href = await btn.get_attribute('href')
            if href and not href.startswith('javascript'):
                if 'amazon' in text or 'kindle' in text:
                    links['amazon'] = href if href.startswith('http') else f"https://www.goodreads.com{href}"
        
        # Check "More options" or list of stores
        if not links.get('amazon'):
            detail_links = await page.query_selector_all('.Button__container--block a')
            for link in detail_links:
                text = (await link.text_content() or "").lower()
                href = await link.get_attribute('href')
                if href and ('amazon' in text or 'kindle' in text):
                    links['amazon'] = href if href.startswith('http') else f"https://www.goodreads.com{href}"
                    
    except Exception as e:
        print(f"Error extracting store links: {e}")
    
    return links


def filter_series_books(book_number: str) -> bool:
    """
    Filter out books that shouldn't be included in series
    Skips: Book 0, Book 0.5, Book ranges (1-2), etc.
    
    Adapted from goodread_book_extra_crawl_copy.py lines 99-100
    
    Args:
        book_number: Book number as string (e.g., "1", "0.5", "1-2")
    
    Returns:
        True if book should be included, False if it should be skipped
    """
    if not book_number:
        return False
    
    book_num_str = str(book_number).strip()
    
    # Skip Book 0
    if book_num_str == '0' or book_num_str.startswith('0.'):
        return False
    
    # Skip .5 books (novellas/side stories)
    if '.5' in book_num_str:
        return False
    
    # Skip ranges (e.g., "1-2")
    if '-' in book_num_str:
        return False
    
    return True


async def extract_goodreads_comprehensive(page: Page) -> Dict[str, any]:
    """
    Extract all available Goodreads data using combined patterns
    
    Args:
        page: Playwright Page object (must already be on Goodreads book page)
    
    Returns:
        Dict with all extracted fields
    """
    # Get JSON-LD first (most reliable)
    json_data = await extract_json_ld_data(page)
    
    # Extract all components
    genres = await extract_genres(page)
    pub_info = await extract_publication_info(page)
    details = await extract_detail_list_items(page)
    pages = await extract_page_count_goodreads(page)
    description = await extract_description_goodreads(page)
    rating_info = await extract_rating_info_goodreads(page)
    
    # Get title and author from JSON-LD or page
    title = json_data.get('name')
    if not title:
        try:
            title_elem = await page.query_selector('h1[data-testid="bookTitle"]')
            if title_elem:
                title = (await title_elem.text_content()).strip()
        except:
            pass
    
    author = None
    if json_data.get('author'):
        if isinstance(json_data['author'], dict):
            author = json_data['author'].get('name')
        elif isinstance(json_data['author'], list) and len(json_data['author']) > 0:
            author = json_data['author'][0].get('name')
    
    if not author:
        try:
            author_elem = await page.query_selector('.ContributorLink__name')
            if author_elem:
                author = (await author_elem.text_content()).strip()
        except:
            pass
    
    # Extract store links (for Amazon bridge)
    store_links = await extract_store_links(page)
    
    return {
        'title': title,
        'author': author,
        'rating': rating_info['rating'],
        'rating_count': rating_info['rating_count'],
        'reviews_count': rating_info['reviews_count'],
        'pages': pages,
        'description': description,
        'genres': ', '.join(genres) if genres else None,
        'primary_genre': genres[0] if genres else None,
        'publication_date': pub_info['publication_date'],
        'original_publication_date': pub_info['original_publication_date'],
        'publisher': pub_info['publisher'] or details['publisher'],
        'isbn': details['isbn'],
        'asin': details['asin'],
        'settings': details['settings'],
        'awards': details['awards'] or details['literary_awards'],
        'original_title': details['original_title'],
        'amazon_bridge_link': store_links.get('amazon'), # New field for bridging
        'json_ld_data': json_data  # Include raw JSON-LD for reference
    }


# Convenience function for testing
async def test_goodreads_extraction(url: str):
    """Test extraction on a specific Goodreads URL"""
    from playwright.async_api import async_playwright
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        await page.goto(url, wait_until='domcontentloaded')
        await page.wait_for_timeout(2000)
        
        data = await extract_goodreads_comprehensive(page)
        
        print("Extracted Goodreads Data:")
        print("=" * 80)
        for key, value in data.items():
            if key != 'json_ld_data':  # Skip raw JSON
                print(f"{key:25s}: {value}")
        
        await browser.close()


if __name__ == "__main__":
    import asyncio
    
    # Test with a sample URL
    test_url = "https://www.goodreads.com/book/show/61431922-pucking-around"
    asyncio.run(test_goodreads_extraction(test_url))
