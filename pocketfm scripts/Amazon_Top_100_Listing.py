"""
Amazon Top 100 Books Scraper using DrissionPage
Genres: Romantasy, Gothic Romance, Paranormal, Vampire, Werewolves & Shifters,
        Witches & Wizards, Angels, Demons & Devils, Ghosts, Magic, Psychic (each Paid + Free).
Extracts: Book name, Author, URL, Ratings, Publisher, Goodreads, Series, Page count, Best Sellers Rank.
"""
from bs4 import BeautifulSoup as bs
import pandas as pd
import os
import time
import re
from DrissionPage import ChromiumPage, ChromiumOptions


# Create output folder
output_folder = "Amazon_Books_Data_30th_Jan_part_1"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)
    print(f"✓ Created folder: {output_folder}")

# Genre configurations: (base_url, genre_label)
# Paid URLs: first page as-is, page 2 with ?pg=2 or &pg=2
# Free URLs: same. Base URL used for page 1; for page 2 we append &pg=2 or ?pg=2
All_genres = [
    # Romantasy
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Romantasy-eBooks/zgbs/digital-text/158569011/ref=zg_bs_nav_digital-text_3_158566011', 'Romantasy_top100_paid'], done 
    # ['https://www.amazon.com/gp/bestsellers/digital-text/158569011/ref=zg_bs?ie=UTF8&tf=1', 'Romantasy_top100_free'],
    # # Gothic Romance
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Gothic-Romance/zgbs/digital-text/6487830011/ref=zg_bs', 'Gothic_Romance_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6487830011/ref=zg_bs?ie=UTF8&tf=1', 'Gothic_Romance_top100_free'],
    # # Paranormal
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Romance/zgbs/digital-text/6190484011/ref=zg_bs', 'Paranormal_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6190484011/ref=zg_bs?ie=UTF8&tf=1', 'Paranormal_top100_free'],
    # # Vampire
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Vampire-Romance/zgbs/digital-text/6190488011/ref=zg_bs_nav_digital-text_4_6190484011', 'Vampire_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6190488011/ref=zg_bs?ie=UTF8&tf=1', 'Vampire_top100_free'],
    # # Werewolves & Shifters
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Werewolves-Shifters-Romance/zgbs/digital-text/6401742011/ref=zg_bs_nav_digital-text_4_6190488011', 'Werewolves_Shifters_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6401742011/ref=zg_bs?ie=UTF8&tf=1', 'Werewolves_Shifters_top100_free'],
    # # Witches & Wizards
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Witches-Wizards-Romance/zgbs/digital-text/6401743011/ref=zg_bs_nav_digital-text_4_6401742011', 'Witches_Wizards_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6401743011/ref=zg_bs?ie=UTF8&tf=1', 'Witches_Wizards_top100_free'],
    # # Angels
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Angel-Romance/zgbs/digital-text/6401737011/ref=zg_bs_nav_digital-text_4_6401743011', 'Angels_top100_paid'],
    ['https://www.amazon.com/gp/bestsellers/digital-text/6401737011/ref=zg_bs?ie=UTF8&tf=1', 'Angels_top100_free'],
    # # Demons & Devils
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Demons-Devils-Romance/zgbs/digital-text/6401738011/ref=zg_bs_nav_digital-text_4_6401737011', 'Demons_Devils_top100_paid'],
    # ['https://www.amazon.com/gp/bestsellers/digital-text/6401738011/ref=zg_bs?ie=UTF8&tf=1', 'Demons_Devils_top100_free'],
    # # Ghosts
    ['https://www.amazon.com/Best-Sellers-Kindle-Store-Paranormal-Ghost-Romance/zgbs/digital-text/6401739011/ref=zg_bs_nav_digital-text_4_6401738011', 'Ghosts_top100_paid'],
    ['https://www.amazon.com/gp/bestsellers/digital-text/6401739011/ref=zg_bs?ie=UTF8&tf=1', 'Ghosts_top100_free'],
    # # Magic
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Magic-Romance-eBooks/zgbs/digital-text/120220984011/ref=zg_bs_nav_digital-text_4_6401739011', 'Magic_top100_paid'],
    ['https://www.amazon.com/gp/bestsellers/digital-text/120220984011/ref=zg_bs?ie=UTF8&tf=1', 'Magic_top100_free'],
    # # Psychic
    # ['https://www.amazon.com/Best-Sellers-Kindle-Store-Psychic-Romance/zgbs/digital-text/6401740011/ref=zg_bs_nav_digital-text_4_120220984011', 'Psychic_top100_paid'],
    ['https://www.amazon.com/gp/bestsellers/digital-text/6401740011/ref=zg_bs?ie=UTF8&tf=1', 'Psychic_top100_free'],
]


def init_driver():
    """Initialize DrissionPage browser"""
    try:
        co = ChromiumOptions()
        page = ChromiumPage(co)
        print("✓ Browser initialized successfully")
        print("📖 Loading amazon.com...")
        page.get('https://www.amazon.com', timeout=15)
        print("✓ Amazon.com loaded")
        print("\n🔐 Please manually login to Amazon in the browser window...")
        print("⏳ Waiting 0 seconds for login...\n")
        for i in range(5, 0, -10):
            print(f"  ⏱️  {i} seconds remaining...")
            time.sleep(10)
        print("✓ Login time completed. Starting scraping process...\n")
        return page
    except Exception as e:
        print(f"✗ Error initializing browser: {e}")
        return None


def get_page_url(base_url, page_num):
    """Return URL for listing page 1 or 2."""
    if page_num <= 1:
        return base_url
    sep = "&" if "?" in base_url else "?"
    return base_url + sep + f"pg={page_num}"


def scroll_listing_page_30_40_30(page, target_count=50, max_attempts=8):
    """
    Scroll listing page in 30%, 40%, 30% increments and keep parsing until we have target_count book URLs.
    Returns (list of book dicts with rank, url), or empty list on failure.
    """
    total_height = page.run_js("return document.body.scrollHeight")
    scroll_30 = total_height * 0.30
    scroll_40 = total_height * 0.40
    scroll_30_2 = total_height * 0.30

    for attempt in range(max_attempts):
        # Scroll 30%
        page.run_js(f"window.scrollBy(0, {scroll_30});")
        time.sleep(1.5)
        # Scroll 40% more (70% total)
        page.run_js(f"window.scrollBy(0, {scroll_40});")
        time.sleep(1.5)
        # Scroll remaining 30% (100%)
        page.run_js(f"window.scrollBy(0, {scroll_30_2});")
        time.sleep(2)
        # Scroll back to top
        page.run_js("window.scrollTo(0, 0);")
        time.sleep(1)

        html = page.html
        soup = bs(html, 'lxml')
        all_books = soup.select('.zg-no-numbers')
        books_data = []
        seen_urls = set()
        for book in all_books:
            try:
                rank_el = book.select_one('.zg-bdg-text')
                rank = rank_el.text.strip() if rank_el else 'N/A'
                link_el = book.select_one('.p13n-sc-uncoverable-faceout a.a-link-normal')
                if not link_el or 'href' not in link_el.attrs:
                    continue
                url = 'https://www.amazon.com' + link_el['href'].split('?')[0].split('#')[0]
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                books_data.append({'rank': rank, 'url': url})
            except Exception:
                continue
        print(f"  📚 Scroll attempt {attempt + 1}: found {len(books_data)} unique books")
        if len(books_data) >= target_count:
            return books_data[:target_count]
    # Page 2 (and others) may have fewer than 50 books — return whatever we found so they get processed
    return books_data if books_data else []


def extract_book_details(soup):
    """Extract all book detail fields including new ones (Series, Book #, Pages, Best Sellers Rank)."""
    data = {
        'title': 'N/A',
        'author_name': 'N/A',
        'no_of_ratings': 'N/A',
        'ratings_ele': 'N/A',
        'publisher': 'N/A',
        'tags': 'N/A',
        'goodreads_rating': 'N/A',
        'goodreads_rating_count': 'N/A',
        'book_series': 'N/A',
        'book_number_in_series': 'N/A',
        'number_of_books_in_series': 'N/A',
        'page_numbers': 'N/A',
        'best_sellers_rank': 'N/A',
    }
    # Title
    try:
        el = soup.select_one('#productTitle')
        if el:
            data['title'] = el.text.strip()
    except Exception:
        pass
    # Author
    try:
        el = soup.select_one('.author .a-link-normal')
        if el:
            data['author_name'] = el.text.strip()
    except Exception:
        pass
    # Customer ratings count
    try:
        el = soup.select_one('#acrCustomerReviewText')
        if el:
            data['no_of_ratings'] = el.text.strip()
    except Exception:
        pass
    # Customer rating score
    try:
        el = soup.select_one('.reviewCountTextLinkedHistogram')
        if el and el.get('title'):
            data['ratings_ele'] = el['title'].split(' ')[0]
    except Exception:
        pass
    # Publisher
    try:
        for data_el in soup.select('ul.a-unordered-list.a-nostyle.a-vertical.a-spacing-none.detail-bullet-list li span.a-list-item'):
            spans = data_el.select('span')
            if len(spans) >= 2 and 'Publisher' in spans[0].text:
                data['publisher'] = spans[1].text.strip()
                break
    except Exception:
        pass
    # Amazon ranking (tags)
    try:
        uls = soup.select('ul.a-unordered-list.a-nostyle.a-vertical.zg_hrsr')
        if uls:
            data['tags'] = uls[-1].text.strip()
    except Exception:
        pass
    if data['tags'] == 'N/A':
        try:
            for row in soup.select('.a-keyvalue.a-vertical-stripes.a-span6 tr'):
                th = row.select_one('th')
                if th and 'Rank' in th.text.strip():
                    tds = row.select('td span')
                    data['tags'] = ' '.join([t.text.strip() for t in tds])
                    break
        except Exception:
            pass
    # Goodreads
    try:
        el = soup.select_one('#reviewFeatureGroup .a-section.gr-review-base .gr-review-rating-text')
        if el:
            data['goodreads_rating'] = el.text.strip().replace('on Goodreads', '')
    except Exception:
        pass
    try:
        el = soup.select_one('#reviewFeatureGroup .a-section.gr-review-base .gr-review-count-text')
        if el:
            data['goodreads_rating_count'] = el.text.strip().replace('ratings', '')
    except Exception:
        pass
    # Book Series (RPI)
    try:
        el = soup.select_one('#rpi-icon-link-book_details-series')
        if el:
            data['book_series'] = el.text.strip()
    except Exception:
        pass
    # Book # in Series & Number of Books in Series (e.g. "Book 1 of 4")
    try:
        el = soup.select_one('#rpi-attribute-book_details-series .rpi-attribute-label span')
        if el:
            text = el.text.strip()
            m = re.match(r'Book\s+(\d+)\s+of\s+(\d+)', text, re.I)
            if m:
                data['book_number_in_series'] = m.group(1)
                data['number_of_books_in_series'] = m.group(2)
    except Exception:
        pass
    # Fallback: detail bullets "Book X of Y"
    if data['book_number_in_series'] == 'N/A':
        try:
            for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
                label = li.select_one('.a-text-bold')
                if label and 'Book' in label.text and 'of' in label.text:
                    text = label.text.strip()
                    m = re.match(r'Book\s+(\d+)\s+of\s+(\d+)', text, re.I)
                    if m:
                        data['book_number_in_series'] = m.group(1)
                        data['number_of_books_in_series'] = m.group(2)
                    link = li.select_one('a.a-link-normal span')
                    if link:
                        data['book_series'] = link.text.strip()
                    break
        except Exception:
            pass
    # Page Numbers (Print length)
    try:
        el = soup.select_one('#rpi-attribute-book_details-ebook_pages .rpi-attribute-value span')
        if el:
            data['page_numbers'] = el.text.strip()
    except Exception:
        pass
    if data['page_numbers'] == 'N/A':
        try:
            el = soup.select_one('#detail_bullets-book_details-ebook_pages')
            if el:
                span = el.select_one('span')
                if span:
                    data['page_numbers'] = span.text.strip()
        except Exception:
            pass
    # Best Sellers Rank from #detailBullets_feature_div li
    try:
        for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
            if not li.select_one('.a-text-bold'):
                continue
            bold = li.select_one('.a-text-bold').text
            if 'Best Sellers Rank' not in bold:
                continue
            parts = []
            # Main line e.g. "#88 Free in Kindle Store (See Top 100...)"
            rest = li.get_text(separator=' ', strip=True)
            # Get nested zg_hrsr list items
            sub_ul = li.select_one('ul.zg_hrsr')
            if sub_ul:
                for sub_li in sub_ul.select('li span.a-list-item'):
                    parts.append(sub_li.get_text(strip=True))
            else:
                # Single line rank
                idx = rest.find('Best Sellers Rank')
                if idx >= 0:
                    parts.append(rest[idx:].replace('Best Sellers Rank :', '').strip())
            data['best_sellers_rank'] = ' | '.join(parts) if parts else rest
            break
    except Exception:
        pass
    return data


# --- Main ---
print(f"\n{'='*60}")
print("🚀 Amazon Top 100 Listing Scraper (Genres: Paid + Free)")
print(f"{'='*60}")
page = init_driver()
if not page:
    print("✗ Failed to initialize driver")
    exit()

CSV_HEADERS = [
    "Book_name", "Book_url", "Author_name", "Amazon_Ranking", "Rank",
    "Customer_rating_count", "Customer_rating", "Goodreads_rating_count", "Goodreads_rating", "Publisher",
    "Book_Series", "Book_Number_in_Series", "Number_of_Books_in_Series", "Page_Numbers", "Best_Sellers_Rank",
]

for genre in All_genres:
    base_url = genre[0]
    genre_label = genre[1]
    headers = {h: [] for h in CSV_HEADERS}
    csv_path = os.path.join(output_folder, f"Amazon_listing_{genre_label}.csv")

    try:
        ans = pd.read_csv(csv_path)
        original_count = len(ans)
        print(f"\n✓ Loaded existing data: {csv_path} ({original_count} books)")
        if 'Book_url' in ans.columns and len(ans) > 0:
            ans = ans.drop_duplicates(subset=['Book_url'], keep='first')
            ans.to_csv(csv_path, index=False)
    except Exception:
        ans = pd.DataFrame(columns=CSV_HEADERS)
        print(f"\n✓ Creating new CSV: {genre_label}")

    processed_urls = set(ans['Book_url'].tolist()) if 'Book_url' in ans.columns and len(ans) > 0 else set()
    print(f"✓ Tracking {len(processed_urls)} already processed URLs")

    new_books_count = 0
    for page_num in [1, 2]:
        list_url = get_page_url(base_url, page_num)
        print(f"\n{'='*60}")
        print(f"📖 {genre_label} — Page {page_num}")
        print(f"{'='*60}")
        try:
            page.get(list_url, timeout=30)
            time.sleep(3)
            books_on_page = scroll_listing_page_30_40_30(page, target_count=50, max_attempts=8)
            if not books_on_page:
                print(f"  ⚠️ No books extracted on page {page_num}")
                continue
            print(f"  📚 Processing {len(books_on_page)} books")
            for idx, b in enumerate(books_on_page):
                book_url = b['url']
                rank_list = b['rank']
                if book_url in processed_urls:
                    continue
                try:
                    page.get(book_url, timeout=20)
                    time.sleep(2)
                    try:
                        last_h = page.run_js("return document.body.scrollHeight")
                        for _ in range(15):
                            page.run_js("window.scrollBy(0, window.innerHeight);")
                            time.sleep(0.4)
                            h = page.run_js("return document.body.scrollHeight")
                            if h == last_h:
                                break
                            last_h = h
                        page.run_js("window.scrollTo(0, 0);")
                        time.sleep(2)
                    except Exception:
                        pass
                    html_book = page.html
                    soup = bs(html_book, 'lxml')
                    d = extract_book_details(soup)
                    ans.loc[len(ans)] = [
                        d['title'], book_url, d['author_name'], d['tags'], rank_list,
                        d['no_of_ratings'], d['ratings_ele'], d['goodreads_rating_count'], d['goodreads_rating'], d['publisher'],
                        d['book_series'], d['book_number_in_series'], d['number_of_books_in_series'], d['page_numbers'], d['best_sellers_rank'],
                    ]
                    processed_urls.add(book_url)
                    new_books_count += 1
                    # Save after each book so progress is never lost
                    ans.to_csv(csv_path, index=False)
                    print(f"    ✓ [{idx+1}] {d['title'][:50]}... (saved to CSV)")
                except Exception as e:
                    print(f"    ✗ Error: {e}")
                    continue
        except Exception as page_err:
            print(f"✗ Page error: {page_err}")
            continue

    print(f"\n✓ Completed {genre_label} — New: {new_books_count}, Total: {len(ans)}")
    print(f"  Saved: {csv_path}\n")

try:
    page.quit()
    print("✓ Browser closed")
except Exception:
    pass
print("\n" + "="*60)
print("✓ ALL GENRES COMPLETED!")
print("="*60)
