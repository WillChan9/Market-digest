from .base_scraper import BaseScraper
from .utils import sanitize_filename, setup_logging, logging
from .macro_handler import S3MacroManager

from datetime import datetime
import time
from urllib.parse import urljoin
import argparse
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver

import pandas as pd
import requests, re, base64 

logger = setup_logging('Troweprice', level=logging.INFO)

class MyScraper(BaseScraper):
    URL = 'https://www.troweprice.com/personal-investing/resources/insights/all-insights.html'
    
    def __init__(self, headless=True):
        super().__init__('Troweprice', 'https://www.troweprice.com', headless=headless)

    def fetch_articles(self):
        response = requests.get(self.URL)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            articles= []
            content_boxes = soup.find_all(class_=['content-box-holder'])
            for box in content_boxes:
                if 'markets & economy' in box.get_text():
                    articles.append(box)
            return articles

    def extract_article_info(self, box):
        match = re.search(r'([a-zA-Z]+)\s(\d{1,2}),\s(\d{4})', box.get_text())
        months = {'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6, 'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12}
        date_text =  f"{int(match.group(3)):04d}-{months[match.group(1).lower()]:02d}-{int(match.group(2)):02d}" if match else None

        # 2. Extract the title
        title = box.find('h3', {'class': 'trp-darkest-gray text-light'}).get_text(strip=True)

        # 3. Extract the article URL
        article_url = box.find('a', {'class': 'content-box-link'})['href']
        full_url = self.base_url + article_url  

        # 4. Extract the description with a failsafe
        description_div = box.find('div', {'class': 'paragraph-md'})
        description = (description_div.find('div', {'class': 'paragraph-contents'}).get_text(strip=True)
                    if description_div and description_div.find('div', {'class': 'paragraph-contents'})
                    else "")
        article_info = {
            'Organization': 'Troweprice',
            'Date': date_text,
            'Title': title.replace(' ', '_'),
            'Link': full_url,
            'Description': description

        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info

    def download_pdf(self, article_info):
        time.sleep(1)  
        pdf_response = requests.get(article_info['Link'])
        self.driver.get(article_info['Link'])
        time.sleep(2)  # wait till loading finish
        result = self.driver.execute_cdp_cmd("Page.printToPDF", {
            "landscape": False,
            "displayHeaderFooter": False,
            "printBackground": True,
            "preferCSSPageSize": True,
        })
        pdf_base64 = result['data']
        with open('tmp/data.pdf', 'wb') as f:
            f.write(base64.b64decode(pdf_base64))

        self.rename_downloaded_file( article_info['file_name'] )
        return True
    
def main(date_from, headless=False, overwrite=False ):

    try:
        date_from = datetime.strptime(date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        return

    articles_index_df = pd.DataFrame(S3MacroManager().get_articles_index())

    scraper = MyScraper(headless=headless) 
    new_articles = scraper.process_articles(articles_index_df, date_from, overwrite)
    scraper.store_articles(new_articles)
    
    logger.info(f"Completed with {len(new_articles)} new articles.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape articles')
    parser.add_argument("-df", "--date_from", type=str, required=True, help='Date (%Y-%m-%d) to scrape back')
    parser.add_argument("--overwrite", action='store_true', help="Re-generate if summary exists (default: False)")
    parser.add_argument("--headless", action='store_true', help="Run browser in headless mode (default: False)")
    args = parser.parse_args()

    # Call the main function with the headless option
    main(args.date_from, args.overwrite, headless=args.headless, overwrite=args.overwrite)