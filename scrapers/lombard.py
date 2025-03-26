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

import pandas as pd
import base64
import requests, os

logger = setup_logging('LombardOdier', level=logging.INFO)

class MyScraper(BaseScraper):
    URL = 'https://www.lombardodier.com/home/about-us/insights.html?categories=investment-insights&tags='
    
    def __init__(self, headless=True):
        super().__init__('LombardOdier', 'https://www.lombardodier.com', headless=headless)

    def fetch_articles(self):
        self.start_browser()
        self.driver.get(self.URL)
        cookie_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "accept"))
            )
        cookie_button.click()
        for _ in range(7):
            self.driver.execute_script("window.scrollBy(0, 2000);")  # Scroll down by 500 pixels
            time.sleep(1)  # Wait for one second
        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        soup = BeautifulSoup(page_source, 'html.parser')
        article_blocks = soup.find_all('div', class_ = 'overviewbloc js-item col-12 col-md-12 col-lg-8')
        return article_blocks

    def extract_article_info(self, article):
        raw_date = article.find("time", class_="overviewbloc-date").get_text(strip=True)
        article_date = datetime.strptime(raw_date, "%B %d, %Y").strftime("%Y-%m-%d")
        article_link = article.find("a", href=True)["href"]
        article_title = article.find("h3", class_="overviewbloc-title").get_text(strip=True)
        article_info = {
                    'Organization': 'LombardOdier',
                    'Date': article_date,
                    'Title': article_title.replace(' ', '_'),
                    'Link': self.base_url + article_link,
                    'Description': '',
                }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info

    def download_pdf(self, article_info):
        self.driver.get(article_info['Link'])
        time.sleep(2)
        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")

        soup = BeautifulSoup(page_source, 'html.parser')
        relative_pdf_link = soup.find('li', class_='sidecontent_inlinedoc').find('a')['href']
        pdf_link = urljoin(self.base_url, relative_pdf_link)

        if pdf_link:
            pdf_response = requests.get( pdf_link)
            if pdf_response.status_code == 200:
                pdf_path = os.path.join('tmp', article_info['file_name'])
                with open(pdf_path, 'wb') as f:
                    f.write(pdf_response.content)
                return True
        else:
            try:
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