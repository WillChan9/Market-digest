from .base_scraper import BaseScraper
from .utils import sanitize_filename, setup_logging, logging
from .macro_handler import S3MacroManager

from datetime import datetime
import time
from urllib.parse import urljoin
import argparse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import pandas as pd
import base64
import requests, os

logger = setup_logging('JPMorgan', level=logging.INFO)

class MyScraper(BaseScraper):
    API_URL_1 = 'https://www.jpmorgan.com/services/json/v1/dynamic-grid.service/parent=jpmorgan/global/US/en/insights/economy&comp=root/content-parsys/dynamic_grid_copy&page=p1.json'
    
    API_URL_2 = 'https://www.jpmorgan.com/services/json/v1/dynamic-grid.service/parent=jpmorgan/global/US/en/insights/markets&comp=root/content-parsys/dynamic_grid_copy_co&page=p1.json'

    def __init__(self, date_from, headless=True):
        super().__init__('JPMorgan', 'https://www.jpmorgan.com', headless=headless)
        self.date_from = date_from

    def fetch_articles(self):
        response = requests.get(self.API_URL_1)
        if response.status_code == 200:
            items1 =  response.json().get("items", [])        
        response = requests.get(self.API_URL_2)
        if response.status_code == 200:
            items2 =  response.json().get("items", [])                
        
        articles = items1 + items2 
        # remove articles < date_from for faster processing
        filtered_articles = []
        for article in articles:
            try:
                # Attempt to parse the date and compare
                article_date = datetime.strptime(article["date"], "%B %d, %Y").strftime("%Y-%m-%d")
                if article_date > self.date_from:
                    filtered_articles.append(article)
            except (KeyError, ValueError):
                # Bypass if 'date' key is missing or the date format is invalid
                continue

        return filtered_articles

    def extract_article_info(self, item):
        article_date = datetime.strptime(item["date"], "%B %d, %Y").strftime("%Y-%m-%d")       
        link = item["link"]
        description = item.get("description", "")
        link = urljoin('https://www.jpmorgan.com', link )
        article_info = {
            'Organization': 'JPMorgan',
            'Date': article_date,
            'Title': item["title"].replace(' ', '_'),
            'Link': link,
            'Description': description,
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")
        return article_info

    def download_pdf(self, article_info):
        if article_info['Link'].endswith('.pdf'):
            pdf_response = requests.get(article_info['Link'])
            if pdf_response.status_code == 200:
                pdf_path = os.path.join('tmp', article_info['file_name'])
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_response.content)
                return True
            
        else:
            try:
                self.driver.get(article_info['Link'])
                WebDriverWait(self.driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )
                time.sleep(5)

                result = self.driver.execute_cdp_cmd("Page.printToPDF", {
                    "landscape": False,
                    "displayHeaderFooter": False,
                    "printBackground": True,
                    "preferCSSPageSize": True,
                })
                pdf_base64 = result['data']
                with open('tmp/data.pdf', 'wb') as f:
                    f.write(base64.b64decode(pdf_base64))

                # Wait for file to be fully written and verify it exists
                WebDriverWait(self.driver, 10).until(lambda x: os.path.exists('tmp/data.pdf') and os.path.getsize('tmp/data.pdf') > 0)

                self.rename_downloaded_file(article_info['file_name'])
                return True
            except Exception as e:
                logger.error(f"Failed to print article: {e}")

        return None
    
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