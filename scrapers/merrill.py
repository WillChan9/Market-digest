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
import requests, os

logger = setup_logging('Merrill', level=logging.INFO)

class MyScraper(BaseScraper):
    URL = 'https://www.ml.com/capital-market-outlook/_jcr_content/bulletin-tilespattern.pagination.recent.json/1.html'
    
    def __init__(self, headless=True):
        super().__init__('Merrill', 'https://www.ml.com', headless=headless)

    def fetch_articles(self):
        response = requests.get(self.URL)
        if response.status_code == 200:
            return response.json().get('pages', [])
        else:
            logger.error(f"Failed to retrieve the page. Status code: {response.status_code}")
            return []

    def extract_article_info(self, article):
        parsed_date = BeautifulSoup(article['author'], 'html.parser').text.strip()
        article_date = datetime.strptime(parsed_date, "%B %d, %Y").strftime("%Y-%m-%d")
        path = article.get('path', 'No Path')
        description = BeautifulSoup(article.get('subtitle', ''), 'html.parser').text
        article_url = f"{self.base_url}/{path}.recent.html"

        article_info = {
            'Organization': 'Merrill',
            'Date': article_date,
            'Title': article['title'].replace(' ', '_'),
            'Link': article_url,
            'Description': description
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info



    def download_pdf(self, article_info):
        self.driver.get(article_info['Link'])
        time.sleep(1)
        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")

        soup = BeautifulSoup(page_source, 'html.parser')        
        pdf_link = soup.find('a', href=lambda href: href and href.endswith('.pdf'))
        pdf_response = requests.get( self.base_url + pdf_link['href'])
        if pdf_response.status_code == 200:
            pdf_path = os.path.join('tmp', article_info['file_name'])
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
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