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
import requests, os, base64
import pandas as pd

logger = setup_logging('SafraSarasin', level=logging.INFO)

class MyScraper(BaseScraper):
    URL = 'https://jsafrasarasin.com/content/jsafrasarasin/language-masters/en/our-perspectives.html'
    
    def __init__(self, headless=True):
        super().__init__('SafraSarasin', self.URL, headless=headless)

    def fetch_articles(self):
        self.driver.get(self.URL)
        time.sleep(4)
        WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.jss-cookieConsent__button.jss-cookieConsent__button--primary"))
        ).click()
        time.sleep(1)
        input_field = WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".autocomplete input[name='input__0']"))
        )
        input_field.click()
        time.sleep(1)
        input_field.send_keys("Switzerland")
        WebDriverWait(self.driver, 10).until(
            EC.visibility_of_element_located((By.ID, "input__0"))
        )
        time.sleep(1)
        self.driver.find_element(By.ID, "input__0").click()
        WebDriverWait(self.driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.jss-cmplf__btn"))
        ).click()
        time.sleep(5)
        page_source = self.driver.execute_script("return document.documentElement.outerHTML;")
    
        soup = BeautifulSoup(page_source, 'html.parser')
        items = soup.find_all('div', class_='jss-cHub--card white')
        
        return items

    def extract_article_info(self, item):
        raw_date = item.find('h4', class_='jss-cHub--card__info--pubDate').text
        article_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")
        article_title = item.find('h2', class_='jss-cHub--card__title').text
        article_description = item.find('p', class_='jss-cHub--card__desc').text
        article_link = item.find('a', class_='jss-cHub--card__link')['href']
        article_link = urljoin(self.base_url, article_link)
        article_info = {
            'Organization': 'SafraSarasin',
            'Date': article_date,
            'Title': article_title.replace(' ', '_'),
            'Link': article_link,
            'Description': article_description
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")        
        return article_info

    def download_pdf(self, article_info):       
        self.driver.get(article_info['Link'])
        time.sleep(2)  # wait till loading finish

        try:  # find the pdf link
            link = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "a.jss-btn__link[href*='https://publications.jsafrasarasin.com']"))
            )
            link.click()
            time.sleep(10)  # wait till download finish
            self.rename_downloaded_file(article_info['file_name'])
            return True
            
        except:  # otherwise print the page
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