import csv
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup as bs
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import ssl
import pandas as pd
import requests
import json
import time
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import re
options = uc.ChromeOptions()
# options.page_load_strategy = 'eager'
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument(f"--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36")

driver = uc.Chrome(options=options)
url ='https://www.goodreads.com/' 
driver.get(url)
time.sleep(30)

def extract_integer(s):
    # Regular expression to find the first integer in the string
    match = re.match(r'\d+', s)
    if match:
        return int(match.group(0))  # Return the matched number as an integer
    return None


input_csv_file = 'Goodreads_historical-romance.csv'
output_csv_file = 'Goodreads_historical-romance_output.csv'

data = []
resume_index = 0

# Resume from the last saved progress if output CSV exists
try:
    existing_data = pd.read_csv(output_csv_file)
    data.extend(existing_data.to_dict('records'))
    resume_index = existing_data.shape[0]
except FileNotFoundError:
    pass
unique_entries = 0
# Process each row in the input CSV
with open(input_csv_file, 'r', newline='' , encoding= 'utf-8') as csvfile:
    csvreader = csv.DictReader(csvfile)

    for _ in range(resume_index):
        next(csvreader)

    row_count = resume_index
    all_series_id = []
    
    for row in csvreader:
        row_count += 1
        series_link = row['series_link']
        if series_link == 'N/A':
            continue
        series_id_data =extract_integer(series_link.split('/')[-1])
        print('series_id', series_id_data)
        # if series_id_data in all_series_id:
        #     continue
        # else:
        #     unique_entries+=1
        #     all_series_id.append(series_id_data)
        print("Processing book URL:", series_link)
        driver.get(series_link)
        time.sleep(3)
        soup = bs(driver.page_source, 'lxml')
        # try:
        #     data_points = soup.select_one('.responsiveSeriesHeader__subtitle').text.strip()
        #     print('data_points', data_points)
        #     primary_works = data_points.split('•')[0].strip() 
        #     total_works = data_points.split('•')[1].strip()
        # except:
        #     primary_works = 'N/A'
        #     total_works =  'N/A'
        #     print('primary_works', primary_works)
        #     print('total_works', total_works)
        all_eight_books_data = soup.select('.listWithDividers__item')
        try:
            primary_works_count = int(row['primary works'].split(' ')[0]) 
            print(' primary_works_count',  primary_works_count)
        except:
            primary_works_count = 1000
        count = 0 
        book_1_link = ''      
        all_books_data = {}
        
        for book in all_eight_books_data :
            try:
                book_no = book.select_one('.gr-h3.gr-h3--noBottomMargin').text.strip()
            except:
                book_no = 'N/A'
            print('book_no', book_no)
            if ('0' in book_no) or ('.5' in book_no) or ('-' in book_no):
                continue
            if (count >=8) or (count >= primary_works_count) :
                break
            count+=1   
            print('count', count, book_no)         
            try:
                series_book_name = book.select_one('a.gr-h3.gr-h3--serif.gr-h3--noMargin  span[itemprop="name"]').text.strip()
            except:
                series_book_name = 'N/A'
            # print('series_book_name',series_book_name )
            book_page_count = 'N/A'  
            try:
                series_book_link = 'https://www.goodreads.com' + book.select_one('a.gr-h3.gr-h3--serif.gr-h3--noMargin')['href']
                
                if count == 1:
                    book_1_link = series_book_link
                else:
                    driver.get(series_book_link)
                    print('series_book_link', series_book_link)
                    time.sleep(1)
                    soup_data = bs(driver.page_source, 'lxml')
                    book_page_count = soup_data.select_one('p[data-testid="pagesFormat"]').text.split(' ')[0]
                    print('book_page_count', book_page_count)
            except:
                series_book_link = 'N/A'
            try:
                series_book_id = series_book_link.split('/')[-1].split('-')[0]   
            except:
                series_book_id = 'N/A'
            book_reviews = 'N/A'
            book_rating_count = 'N/A'
            book_editions_count ='N/A'
            book_publish_date = 'N/A'
            try:
                book_rating = book.select_one('.communityRating__stars')['aria-label'] 
            except:
                book_rating = 'N/A'
            try:
                all_data = book.select('.u-inlineBlock')
                for book_data_point in all_data:
                    text = book_data_point.text.strip()
                    # print('text', text)
                    if 'edition' in text:
                        book_editions_count = text
                    if 'Review' in text:
                        book_reviews = text
                    if 'Rating' in text:
                        book_rating_count = text
                    if ('publish' in text) or ('public' in text):
                        book_publish_date = text
            except Exception as E:
                 print('Error fetching details', E)
            book_wise_dict = {f"book_{count}_link": series_book_link, 
                            f"book_{count}_name":series_book_name, 
                            f"book_{count}_rating":book_rating, 
                            f"book_{count}_reviews":book_reviews, 
                            f"book_{count}_rating_count":book_rating_count, 
                            f"book_{count}_editions_count":book_editions_count, 
                            f"book_{count}_first_published":book_publish_date,
                            f"book_{count}_page_count":book_page_count,    
                            }
            # print('book_wise_dict', book_wise_dict)
            all_books_data.update(book_wise_dict)      
        try:
            print('book_1_link', book_1_link)
            driver.get(book_1_link)
            # if unique_entries==1:
            #     time.sleep(10)
            # else:
            #     time.sleep(3)
            soup4 = bs(driver.page_source, 'lxml')
        except:
            pass
        try:
            data_given = soup4.select_one('script[type="application/ld+json"]').text
            json_data = json.loads(data_given)
        except Exception as E:
            print(f"Error:can't get json_data,{E}")
            
        # print('json_data' , json_data)
        # try:
        #     data_2 = soup4.select_one('#__NEXT_DATA__').text
        #     json_data_two = json.loads(data_2) 
        #     legacy_id = series_book_id 
        #     # print('legacy_id',legacy_id ) 
        #     book_ref = json_data_two.get('props').get('pageProps').get('apolloState').get('ROOT_QUERY')
        #     # print('book_ref', book_ref)
        #     book_ref = json_data_two.get('props').get('pageProps').get('apolloState').get('ROOT_QUERY').get(f'getBookByLegacyId({{"legacyId":"{legacy_id}"}})').get('__ref')
        #     book_details = json_data_two.get('props').get('pageProps').get('apolloState').get(str(book_ref))
        #     work_ref = book_details.get('work').get('__ref')
        #     work_details = json_data_two.get('props').get('pageProps').get('apolloState').get(work_ref)
        # except:
        #     pass
        try:
            first_book_editions_count = book.select_one('.communityRating .gr-metaText .gr-hyperlink').text.strip()
        except:
            first_book_editions_count = 'N/A'
        try:
            first_book_rating = soup4.select_one('.RatingStatistics__rating').text.strip()
        except:
            first_book_rating = 'N/A'
        try:
            first_book_reviews = soup4.select_one('span[data-testid="ratingsCount"]').text.strip().split('\xa0')[0]
        except:
            first_book_reviews = 'N/A'
        try:
            first_book_rating_count = soup4.select_one('span[data-testid="reviewsCount"]').text.split('\xa0')[0]
        except:
            first_book_rating_count = 'N/A'
        try: 
            first_book_reading_list =  soup4.select_one('div[data-testid="toReadSignal"]').text.strip().split(' ')[0]
        except:
            first_book_reading_list = 'N/A'
        try:
            first_book_pages_count = json_data.get('numberOfPages') or soup4.select_one('p[data-testid="pagesFormat"]').text.split(' ')[0]
        except:
            first_book_pages_count = 'N/A'
        try:
            book_genre = soup4.select('.BookPageMetadataSection__genres .Button__labelItem')
            genre_list = [genre.text.strip() for genre in book_genre]
            first_book_genres_comma_separated = ', '.join(genre_list)
        except:
            first_book_genres_comma_separated = 'N/A'
        try:
            first_book_publish_date = soup4.select_one('p[data-testid="publicationInfo"]').text.split('published')[-1]
        except:
            first_book_publish_date = 'N/A'
        try:
            first_book_summary = soup4.select_one('.BookPageMetadataSection__description .DetailsLayoutRightParagraph__widthConstrained .Formatted').text.strip()
        except:
            first_book_summary = 'N/A'
        try:
            first_book_awards = json_data.get('awards')
        except:
            first_book_awards = 'N/A'
        # try:
        #     settings_list = work_details.get('details').get('places')
        #     for setting in settings_list:
        #         # print('setting',setting )
        #         places_names = [place['name'] for place in data['places']]
        #     first_book_settings = ', '.join(places_names)
        # except:
        #     first_book_settings = 'N/A'
        # try:
        #     first_book_ISBN = book_details.get('details').get('isbn13') 
        # except:
        #     first_book_ISBN = 'N/A'
        # try:
        #     first_book_ASIN = book_details.get('details').get('asin')
        # except:
        #     first_book_ASIN = 'N/A'
        first_book_settings = 'N/A'
        first_book_ISBN = 'N/A'
        first_book_ASIN = 'N/A'
        first_book_publisher = 'N/A'
        try:
            data_list = soup4.select('.CollapsableList .DescListItem')
            for list in data_list:
                type = list.select_one('dt').text
                print('type', type)
                data_val = list.select_one('dd').text
                if 'ISBN' in type  :
                    first_book_ISBN = data_val
                if 'ASIN' in type :
                    first_book_ASIN = data_val
                if 'Setting' in type :
                    first_book_settings = data_val.replace('\xa0', '')
                if ('awards') in type and (not first_book_awards):
                    first_book_awards = data_val
                if 'Publish' in type:
                    first_book_publisher = data_val.split('by')[-1] or data_val

            print('first_book_ISBN', first_book_ISBN ,'first_book_ASIN ',  first_book_ASIN, 'first_book_settings' , first_book_settings, 
                'first_book_awards',  first_book_awards, 'first_book_publisher', first_book_publisher )
        except Exception as E:
            print('Error', E)       
                                                                                                
                                                                                       
        row_data = {
            'Series_link': row['series_link'],
            'Series_name': row['Series_name'],
            'primary works': row['primary works'],
            'total works': row['total works'],
            'author links': row['author links'],
            'author_name': row['author_name'],
            'author_followers': row['author_followers'],
            'author_books': row['author_books'],
            "book_1_URL": all_books_data.get('book_1_link'), 
            "book_1_name": all_books_data.get('book_1_name'), 
            "book_1_rating":first_book_rating, 
            "book_1_rating_count":first_book_rating_count, 
            "book_1_reviews":first_book_reviews, 
            "book_1_page_count":first_book_pages_count, 
            "book_1_editions_count":all_books_data.get('book_1_editions_count') , 
            "book_1_summary": first_book_summary, 
            "book_1_genre (first 5 only)": first_book_genres_comma_separated, 
            "book_1_first_published":first_book_publish_date, 
            "book_1_Awards":first_book_awards, 
            "book_1_setting":first_book_settings, 
            "book_1_ISBN":first_book_ISBN, 
            "book_1_ASIN":first_book_ASIN,
            "book_1_publisher": first_book_publisher, 
            "book_2_URL": all_books_data.get('book_2_link'), 
            "book_2_name": all_books_data.get('book_2_name'), 
            "book_2_rating":all_books_data.get('book_2_rating') , 
            "book_2_rating_count":all_books_data.get('book_2_rating_count'), 
            "book_2_reviews":all_books_data.get('book_2_reviews'),  
            "book_2_editions_count":all_books_data.get('book_2_editions_count'),  
            "book_2_first_published":all_books_data.get('book_2_first_published'), 
            "book_2_page_count":all_books_data.get('book_2_page_count'),
            "book_3_URL": all_books_data.get('book_3_link'), 
            "book_3_name": all_books_data.get('book_3_name'), 
            "book_3_rating":all_books_data.get('book_3_rating'), 
            "book_3_rating_count":all_books_data.get('book_3_rating_count'), 
            "book_3_reviews":all_books_data.get('book_3_reviews'), 
            "book_3_editions_count":all_books_data.get('book_3_editions_count'), 
            "book_3_first_published":all_books_data.get('book_3_first_published'), 
            "book_3_page_count":all_books_data.get('book_3_page_count'),
            "book_4_URL": all_books_data.get('book_4_link'), 
            "book_4_name": all_books_data.get('book_4_name'), 
            "book_4_rating":all_books_data.get('book_4_rating'), 
            "book_4_rating_count":all_books_data.get('book_4_rating_count'), 
            "book_4_reviews":all_books_data.get('book_4_reviews'), 
            "book_4_editions_count":all_books_data.get('book_4_editions_count'), 
            "book_4_first_published":all_books_data.get('book_4_first_published'),       
            "book_4_page_count":all_books_data.get('book_4_page_count'),
            "book_5_URL":all_books_data.get('book_5_link'), 
            "book_5_name":all_books_data.get('book_5_name'), 
            "book_5_rating":all_books_data.get('book_5_rating'), 
            "book_5_rating_count":all_books_data.get('book_5_rating_count'), 
            "book_5_reviews":all_books_data.get('book_5_reviews'), 
            "book_5_editions_count":all_books_data.get('book_5_editions_count'),        
            "book_5_first_published":all_books_data.get('book_5_first_published'),
            "book_5_page_count":all_books_data.get('book_5_page_count'),
            "book_6_URL":all_books_data.get('book_6_link'), 
            "book_6_name":all_books_data.get('book_6_name'), 
            "book_6_rating":all_books_data.get('book_6_rating'), 
            "book_6_rating_count":all_books_data.get('book_6_rating_count'), 
            "book_6_reviews":all_books_data.get('book_6_reviews'), 
            "book_6_editions_count":all_books_data.get('book_6_editions_count'),        
            "book_6_first_published":all_books_data.get('book_6_first_published'),
            "book_6_page_count":all_books_data.get('book_6_page_count'),
            "book_7_URL":all_books_data.get('book_7_link'), 
            "book_7_name":all_books_data.get('book_7_name'), 
            "book_7_rating":all_books_data.get('book_7_rating'), 
            "book_7_rating_count":all_books_data.get('book_7_rating_count'), 
            "book_7_reviews":all_books_data.get('book_7_reviews'), 
            "book_7_editions_count":all_books_data.get('book_7_editions_count'),        
            "book_7_first_published":all_books_data.get('book_7_first_published'),      
            "book_7_page_count":all_books_data.get('book_7_page_count'),
            "book_8_URL":all_books_data.get('book_8_link'), 
            "book_8_name":all_books_data.get('book_8_name'), 
            "book_8_rating":all_books_data.get('book_8_rating'), 
            "book_8_rating_count":all_books_data.get('book_8_rating_count'), 
            "book_8_reviews":all_books_data.get('book_8_reviews'), 
            "book_8_editions_count":all_books_data.get('book_8_editions_count'),        
            "book_8_first_published":all_books_data.get('book_8_first_published'),            
            "book_8_page_count":all_books_data.get('book_8_page_count')
        }   
       
        data.append(row_data)

        df = pd.DataFrame(data)
        df.to_csv(output_csv_file, index=False)

        print(f"Processed row {row_count} successfully.")
    

driver.quit()
# print('unique entries', unique_entries )
print(f"Data has been collected and saved to {output_csv_file}.")