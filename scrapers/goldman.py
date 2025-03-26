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
import requests

logger = setup_logging('Goldman', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://am.gs.com/en-us/institutions/insights/topics/macroeconomics"
    API_URL = 'https://am.gs.com/services/search-engine/en-us/institutions/search/insights?hitsPerPage=15&tags=product%2Fmacroeconomics'

    def __init__(self, date_from, headless=True):
        super().__init__('Goldman', self.ARTICLE_URL, headless=headless)
        self.date_from = date_from

    def fetch_articles(self):
        self.start_browser()  

        self.driver.get(self.ARTICLE_URL)
        institution_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.ID, "button-select-institutions"))
            )
        institution_button.click()
        response = requests.get(self.API_URL)
        if response.status_code == 200:
            data = response.json()

            # remove articles < date_from for faster processing
            articles = data['insights']['hits']
            filtered_articles = [article for article in articles if datetime.strptime(article['publishDate'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d") > self.date_from]

            return filtered_articles

    def extract_article_info(self, article):
        parsed_datetime = datetime.fromisoformat(article['publishDate'].replace('Z', '+00:00'))
        formatted_date = parsed_datetime.strftime('%Y-%m-%d')
        article_info = {
                'Organization': 'GoldmanSachs',
                'Date': formatted_date,
                'Title': article['title'].replace(' ', '_'),
                'Link': 'https://am.gs.com' + article['slug'],
                'Description': article.get('summaryTeaserText', '')
            }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")                        

        return article_info


    def download_pdf(self, article_info):

        self.driver.execute_script("window.open('');")
        self.driver.switch_to.window(self.driver.window_handles[1])
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

        self.rename_downloaded_file(article_info['file_name'])
        return True
    
def main(date_from, headless=False, overwrite=False ):

    try:
        date_from = datetime.strptime(date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        return

    articles_index_df = pd.DataFrame(S3MacroManager().get_articles_index())

    scraper = MyScraper(date_from = date_from, headless=headless) 
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