from .base_scraper import BaseScraper
from .utils import sanitize_filename, setup_logging, logging, extract_article_info_from_pdf
from .macro_handler import S3MacroManager

from datetime import datetime
from urllib.parse import urljoin
import argparse
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains
import pandas as pd
import os

from .utils import parse_text_from_pdf
import shutil

logger = setup_logging('WisdomTree', level=logging.INFO)

local_db_path = 'local_db/wisdom_tree'

class MyScraper(BaseScraper):
    URL = 'https://www.wisdomtree.com/us/en/insights/all-insights'
    
    def __init__(self, headless=True):
        super().__init__('WisdomTree', self.URL, headless=headless)

    def fetch_articles(self):
        logger.debug("Fetching articles from WisdomTree local_db folder")
        articles = []
        local_db_path = 'local_db/wisdom_tree'
        
        if os.path.exists(local_db_path):
            for filename in os.listdir(local_db_path):
                if filename.endswith('.pdf'):
                    file_path = os.path.join(local_db_path, filename)
                    # Extract information from the PDF filename
                    filename_without_extension = os.path.splitext(filename)[0]
                    article_data = {
                        'PublishDate': '',
                        'Title': '',
                        'Filename': filename_without_extension,
                        'PostUrl': f'file://{file_path}',
                        'Description': ''  # You might want to extract this from the PDF content
                    }
                    articles.append(article_data)
        if not articles:
            logger.warning("No articles found in local_db/wisdom_tree folder")
        else:
            logger.debug(f"Retrieved {len(articles)} articles from local database")
        return articles
            

    def extract_article_info(self, article):
        pdf_path = os.path.join(local_db_path, article['Filename']+ ".pdf")
        text = parse_text_from_pdf(pdf_path)

        article_info_from_pdf = extract_article_info_from_pdf(text)
        article_info = {
            'Organization': 'WisdomTree',
            'Date': article_info_from_pdf['Date'],
            'Title': article_info_from_pdf['Title'].replace(' ', '_'),
            'Link': article["PostUrl"],
            'Description': article_info_from_pdf['Description']
        }
        article_info['file_name'] = sanitize_filename(f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf")    
        return article_info

    def download_pdf(self, article_info):  
        # Copy the PDF file
        try:
            source_path = article_info['Link'].replace('file://', '')
            destination_path = os.path.join('tmp', article_info['file_name'])
            shutil.copy2(source_path, destination_path)
        except IOError as e:
            self.logger.error(f"Failed to copy PDF from {source_path} to {destination_path}: {e}")
            return None

        self.logger.info(f"PDF copied and saved as {destination_path}")
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