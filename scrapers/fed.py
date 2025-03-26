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
import requests, os, re

logger = setup_logging('FED', level=logging.INFO)

class MyScraper(BaseScraper):
    ARTICLE_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    
    def __init__(self, headless=True):
        super().__init__('FED', self.ARTICLE_URL, headless=headless)

    def fetch_articles(self):
        self.start_browser()  # Start browser and load cookies
        # self.driver.get(self.ARTICLE_URL)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(self.ARTICLE_URL, headers=headers)
        try:
            soup = BeautifulSoup(response.content, 'html.parser')          
            pdf_divs = soup.find_all('a', href=lambda href: href and href.endswith('.pdf'))
            meetings = [] 
            # Iterate through all the PDF links and find the parent div it belongs to
            for pdf in pdf_divs:
                # Get the parent div for each PDF link
                parent_div = pdf.find_parent('div')
                meetings.append(parent_div)
            return meetings
          
        except Exception as e:
            logger.error(f"Error fetching articles: {e}")
            return []

    def extract_article_info(self, meeting):
        article_info = None
        try:
            statementLink = [a['href'] for a in meeting.find_all('a', string='PDF') if 'files/monetary' in a['href'] ]
            minutesLink = [a['href'] for a in meeting.find_all('a', string='PDF') if 'files/fomcminutes' in a['href'] ]

            if statementLink:
                date_str = statementLink[0].split('/')[-1].split('monetary')[1][:8]
                article_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
                article_info = {
                    'Organization': 'FED',
                    'Date': article_date,
                    'Title': 'Federal Reserve Press Release',
                    'Link': f"https://www.federalreserve.gov{statementLink[0]}",
                    'Description': f'Federal Reserve Press Release for {article_date} meeting'
                }
                article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")
            
            if minutesLink:
                element = meeting.text
                date_match = re.search(r"(\w+\s\d{1,2},\s\d{4})", element)
                date_str = date_match.group(1)
                article_date = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")     
                meeting_date_str = minutesLink[0].split('/')[-1].split('fomcminutes')[1][:8]
                meeting_date = datetime.strptime(meeting_date_str, "%Y%m%d").strftime("%Y-%m-%d")
                article_info = {
                    'Organization': 'FED',
                    'Date': article_date,
                    'Title': 'Federal Reserve Minutes',
                    'Link': f"https://www.federalreserve.gov{minutesLink[0]}",
                    'Description': f"Federal Reserve Minutes for {meeting_date} meeting released on {article_date}"
                }     
                article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")                
            return article_info
        
        except Exception as e:
            self.logger.error(f"Error extracting article info: {e}")
            return {}


    def download_pdf(self, article_info):
        if article_info['Link'].endswith('.html'):
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
        elif article_info['Link'].endswith('.pdf'):
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