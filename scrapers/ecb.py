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

logger = setup_logging('ECB', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://www.ecb.europa.eu/press/pr/activities/mopo/html/index.en.html"

    def __init__(self, date_from, headless=True):
        super().__init__('ECB', 'https://www.ecb.europa.eu', headless=headless)
        self.date_from = date_from 

    def fetch_articles(self):
        def flatten_dd(dd_element, dt_element):
            """
            Recursively flatten <dd> elements if they contain other <dd> children.
            """
            # Store the initial dd element
            rows = [[str(dt_element), str(dd_element)]]
            
            # Find any nested <dd> elements inside the current <dd>
            child_dds = dd_element.find_all('dd', recursive=False)  # Use recursive=False to prevent going too deep in one pass
            for child_dd in child_dds:
                rows.extend(flatten_dd(child_dd, dt_element))  # Recursively flatten nested <dd> elements

            return rows
                
        self.start_browser()  # Start browser and load cookies
        self.driver.get(self.ARTICLE_URL)

        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        soup = BeautifulSoup(page_source, 'html.parser')
        definition_list = soup.find_all('dl')
        rows = []
        for dl in definition_list:
            dt_elements = dl.find_all('dt')
            dd_elements = dl.find_all('dd')

            # Pair each <dt> with its corresponding <dd>
            for dt, dd in zip(dt_elements, dd_elements):
                article_date = dt.get('isodate', None)
                if article_date and article_date < self.date_from:
                    break
                
                # Flatten the <dd> element (including any child <dd> elements)
                rows.extend(flatten_dd(dd, dt))

        return rows
          


    def extract_article_info(self, row):
        dt = BeautifulSoup(row[0], 'html.parser')
        dd = BeautifulSoup(row[1], 'html.parser')
        article_date = dt.find('dt')['isodate']

        article_div = dd.find('dd')
        title_div = article_div.find('div', class_='title')  
        link = title_div.find('a')  # Find the link within the title 
        article_link = urljoin('https://www.ecb.europa.eu/', link['href'])
        article_title = link.text.strip()
        article_info = {
            'Organization': 'ECB',
            'Date': article_date,
            'Title': article_title.replace(' ', '_'),
            'Link': article_link,
            'Description': '',
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info


    def download_pdf(self, article_info):
        if article_info['Link'].endswith('.html'):
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
        elif '.pdf' in article_info['Link']:
            pdf_response = requests.get(article_info['Link'])
            if pdf_response.status_code == 200:
                pdf_path = os.path.join('tmp', article_info['file_name'])
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_response.content)
                return True
        else:
            self.logger.warning(f"No PDF link found for {article_info['Title']}")
        return None

def main(date_from, headless=False, overwrite=False ):

    try:
        date_from = datetime.strptime(date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        return

    articles_index_df = pd.DataFrame(S3MacroManager().get_articles_index())

    scraper = MyScraper(date_from= date_from, headless=headless) 
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