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
import requests, os, base64
import time, random

logger = setup_logging('MorganStalney', level=logging.ERROR)

class MyScraper(BaseScraper):
    URL = 'https://www.morganstanley.com/im/en-us/institutional-investor/insights.html'
    
    def __init__(self, headless=True):
        super().__init__('MorganStalney', self.URL, headless=headless)

    def fetch_articles(self):
        self.logger.debug("Navigating to URL")
        self.driver.get(self.URL)
            
        accept_all_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "_evidon-accept-button"))
        )
        self.logger.debug("Found cookie acceptance button")
        accept_all_button.click()
        self.logger.debug("Clicked cookie acceptance button")
            
        page_content = self.driver.page_source
        soup = BeautifulSoup(page_content, 'html.parser')
        article_entries = soup.find_all('div', class_='borderBottom borderBottomSm borderBottomXs noPadding noMargin row')
        
        self.logger.debug(f"Found {len(article_entries)} articles")
        return article_entries


    def extract_article_info(self, entry):
        date_span = entry.find('span', class_='pressCenterDate')
        date_text = date_span.get_text(strip=True).replace('•\xa0', '').strip()
        try:
            article_date = datetime.strptime(date_text, '%b %d, %Y')
        except ValueError:
            logger.error(f"Date parsing error: {date_text}")
            return 
        title = entry.find('h4', class_='media-heading')
        author_div = entry.find('div', class_='insightAuthorName')
        abstract_div = entry.find('div', class_='pressCenterText')
        dt = article_date.strftime('%Y-%m-%d')
        link = title.find('a')['href'] if title and title.find('a') else 'No Link'
        article_info = {
            'Organization': 'MorganStanley',
            'Date': dt,
            'Title': title.find('a').get_text(strip=True).replace(' ', '_'),
            'Author': author_div.get_text(strip=True) if author_div else 'No Author',
            'Description': abstract_div.get_text(strip=True) if abstract_div else 'No Abstract',
            'Link': 'https://www.morganstanley.com' + link,
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info

    def download_pdf(self, article_info):
        self.driver.get(article_info['Link'])
        time.sleep(1)
        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")

        soup = BeautifulSoup(page_source, 'html.parser')        
        pdf_link = soup.find('a', href=lambda href: href and '.pdf' in href)
        if pdf_link:
            pdf_response = requests.get( 'https://www.morganstanley.com' + pdf_link['href'])
            time.sleep(2)
            if pdf_response.status_code == 200:
                pdf_path = os.path.join('tmp', article_info['file_name'])
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_response.content)
                return True
        else:
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