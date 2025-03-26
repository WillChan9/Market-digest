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
import base64
import requests, os

logger = setup_logging('BIS', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://www.bis.org/quarterlyreviews/index.htm"
    BASE_URL = 'https://www.bis.org'

    def __init__(self, date_from, headless=True):
        super().__init__('BIS', self.BASE_URL, headless=headless)
        self.date_from = date_from

    def fetch_articles(self):
        self.start_browser()  # Start browser and load cookies
        self.driver.get(self.ARTICLE_URL)
        time.sleep(2)
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        items = soup.find_all("tr", class_=["item even", "item odd"])
        return items

    def extract_article_info(self, item):
        date_div = item.find('td', class_='item_date')
        article_date = date_div.text.strip() if date_div else ''
        article_date = datetime.strptime(article_date, '%d %b %Y').strftime('%Y-%m-%d')
        
        title_div = item.find('div', class_='title')
        link = title_div.find('a') if title_div else None
        article_title = link.text.strip() if link else ''
        article_link = urljoin(self.BASE_URL, link['href']) if link else ''
        
        article_info = {
            'Organization': 'BIS',
            'Date': article_date,
            'Title': article_title.replace(' ', '_'),
            'Link': article_link,
            'Description': '',
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")
        return article_info

    def download_pdf(self, article_info):
        pdf_link = article_info['Link'].replace('.htm', '.pdf')
        pdf_response = requests.get(pdf_link)
        if pdf_response.status_code == 200:
            pdf_path = os.path.join('tmp', article_info['file_name'])
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
            return True
        else:
            # Try to find the PDF link from the article page
            self.driver.get(article_info['Link'])
            time.sleep(2)
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            pdf_link_tag = soup.find('a', text='Download the PDF version')
            if pdf_link_tag:
                pdf_link = urljoin(self.BASE_URL, pdf_link_tag['href'])
                pdf_response = requests.get(pdf_link)
                if pdf_response.status_code == 200:
                    pdf_path = os.path.join('tmp', article_info['file_name'])
                    with open(pdf_path, 'wb') as f:
                        f.write(pdf_response.content)
                    return True
        self.logger.warning(f"Failed to download PDF for {article_info['Title']}")
        return False

def main(date_from, headless=False, overwrite=False):
    try:
        date_from = datetime.strptime(date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        return

    articles_index_df = pd.DataFrame(S3MacroManager().get_articles_index())

    scraper = MyScraper(date_from=date_from, headless=headless)
    new_articles = scraper.process_articles(articles_index_df, date_from, overwrite)
    scraper.store_articles(new_articles)
    
    logger.info(f"Completed with {len(new_articles)} new articles.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape articles from BIS')
    parser.add_argument("-df", "--date_from", type=str, required=True, help='Date (%Y-%m-%d) to scrape back')
    parser.add_argument("--overwrite", action='store_true', help="Re-generate if summary exists (default: False)")
    parser.add_argument("--headless", action='store_true', help="Run browser in headless mode (default: False)")
    args = parser.parse_args()

    # Call the main function with the headless option
    main(date_from=args.date_from, headless=args.headless, overwrite=args.overwrite)