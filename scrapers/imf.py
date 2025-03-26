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

logger = setup_logging('IMF', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://www.imf.org/en/Publications"

    def __init__(self, date_from, headless):
        super().__init__('IMF', 'https://www.imf.org', headless=headless)
        self.date_from = date_from 

    def fetch_articles(self):
        self.start_browser()  # Start browser and load cookies
        self.driver.get(self.ARTICLE_URL)
        time.sleep(2)  # Wait for page to load

        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
        soup = BeautifulSoup(page_source, 'html.parser')

        article_elements = soup.find_all('div', class_='cell belt-item')
        articles = []
        for article_element in article_elements:
            articles.append(article_element)
        return articles

    def extract_article_info(self, article_element):
        # Extract title and link from the <h3> element
        h3_element = article_element.find('h3')
        if h3_element:
            title_link = h3_element.find('a')
            if title_link:
                article_title = title_link.get_text(strip=True)
                article_link = urljoin('https://www.imf.org', title_link.get('href', ''))
            else:
                article_title = 'No Title'
                article_link = ''
        else:
            article_title = 'No Title'
            article_link = ''
        
        # Extract date from the <p class="date"> element
        date_p = article_element.find('p', class_='date')
        if date_p:
            date_text = date_p.get_text(strip=True)
            # Try parsing date in different formats
            date_formats = ['%B %Y', '%B %d, %Y']
            article_date = ''
            for fmt in date_formats:
                try:
                    date_parsed = datetime.strptime(date_text, fmt)
                    if fmt == '%B %Y':
                        # Set day to 1 if only month and year are provided
                        date_parsed = date_parsed.replace(day=1)
                    article_date = date_parsed.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue
            else:
                # Default date if parsing fails
                article_date = ''
        else:
            article_date = ''
        
        # Extract description from the next <p> element after date_p
        if date_p:
            description_p = date_p.find_next_sibling('p')
        else:
            description_p = None
        if description_p:
            description_link = description_p.find('a')
            if description_link:
                article_description = description_link.get_text(strip=True)
            else:
                article_description = description_p.get_text(strip=True)
        else:
            article_description = ''
        
        article_info = {
            'Organization': 'IMF',
            'Date': article_date,
            'Title': article_title.replace(' ', '_'),
            'Link': article_link,
            'Description': article_description,
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info


    def download_pdf(self, article_info):
        main_window = self.driver.current_window_handle
    # Open the article link in a new tab
        self.driver.execute_script("window.open('{}');".format(article_info['Link']))
        
        # Switch to the new tab
        new_window = [window for window in self.driver.window_handles if window != main_window][0]
        self.driver.switch_to.window(new_window)
        
        try:

            # Find the PDF link that contains 'media/Files/Publications/'
            pdf_link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a[href*='media/Files/Publications/']")
                )
            )
            pdf_link.click()  # Click the PDF link to start download
            time.sleep(10)  # Wait for the download to finish
            
            # Close the article tab
            self.driver.close()
            self.driver.switch_to.window(main_window)
            
            # Rename the downloaded file
            self.rename_downloaded_file(article_info['file_name'])
            return True
        
        except Exception as e:
            self.logger.warning(f"Failed to download PDF for {article_info['Title']}: {e}")
            # Close the article tab if an exception occurs
            self.driver.close()
            self.driver.switch_to.window(main_window)
            return False

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
    main(args.date_from, headless=args.headless, overwrite=args.overwrite)
