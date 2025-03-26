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

logger = setup_logging('BlackRock', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://www.blackrock.com/corporate/insights/blackrock-investment-institute/archives#weekly-commentary"

    def __init__(self, headless=False):
        super().__init__('BlackRock', 'https://www.blackrock.com', headless=headless)

      
    def fetch_articles(self):
        self.driver.get(self.ARTICLE_URL)

        try:
            # Accept cookies if they appear
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept all")]'))
                ).click()

                WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept")]'))
                ).click()

                # Save the cookies after accepting terms and conditions
                self.save_cookies()
            except Exception as e:
                logger.info("Cookies already accepted or no prompt found.")

            # Load more articles
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//a[contains(@class, "load-more")]'))
            ).click()

            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            return soup.find_all('div', class_='item', style=lambda value: 'display: block' in value if value else False)

        except Exception as e:
            logger.error(f"Error fetching articles: {e}")
            return []

    def extract_article_info(self, article):
        """Extract information such as title, date, and PDF link."""
        try:
            article_date = datetime.strptime(article.find('div', class_='attribution').get_text(strip=True), "%b %d, %Y").strftime("%Y-%m-%d")
            title = article.find('h2', class_='title').get_text(strip=True)
            pdf_link = urljoin(self.base_url, article.find('a')['href']) if article.find('a') else ''
            article_info = {
                'Organization': 'BlackRock',
                'Date': article_date,
                'Title': title.replace(' ', '_'),
                'Link': pdf_link,
            }
            article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")
            return article_info
        except Exception as e:
            self.logger.error(f"Error extracting article info: {e}")
            return {}


    def download_pdf(self, article_info):
        """Implement the unique download logic for BlackRock."""
        self.start_browser()  # Start browser from BaseScraper
        pdf_link = article_info.get('Link')
        if pdf_link:
            try:
                self.driver.get(pdf_link)
                time.sleep(2)
                self.rename_downloaded_file(article_info['file_name'])
                return True
            except Exception as e:
                self.logger.error(f"Failed to download PDF for {article_info['Title']}: {e}")
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
