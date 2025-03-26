from .base_scraper import BaseScraper
from .utils import setup_logging, logging
from .macro_handler import S3MacroManager
from datetime import datetime
import requests
import pandas as pd
import argparse
import os
from urllib.parse import unquote, urljoin

# Set up logging
logger = setup_logging('BISScraper', level=logging.INFO)

class MyScraper(BaseScraper):
    API_ENDPOINTS = [
        "https://www.bis.org/api/tables/homepage_speeches_cbspeeches.json",
        "https://www.bis.org/api/tables/homepage_speeches_bisspeeches.json",
        "https://www.bis.org/api/tables/homepage_research_bispubls.json",
        "https://www.bis.org/api/tables/homepage_statistics_statspubls.json"
    ]
    BASE_URL = "https://www.bis.org"

    def __init__(self, date_from, headless):
        super().__init__('BIS', self.BASE_URL, headless=headless)
        self.date_from = date_from 

    def fetch_articles(self):
        articles = []
        for url in self.API_ENDPOINTS:
            logger.info(f"Fetching data from {url}")
            response = requests.get(url)
            response.raise_for_status()
            articles.extend(response.json())
        return articles

    def extract_article_info(self, item):
        article_date = datetime.strptime(unquote(item['date']), "%d %b %Y").strftime("%Y-%m-%d")
        article_title = unquote(item['title']).replace(' ', '_')
        article_link = urljoin(self.BASE_URL, unquote(item['link']))
        
        article_info = {
            'Organization': 'BIS',
            'Date': article_date,
            'Title': article_title,
            'Link': article_link,
            'Description': ''
        }
        article_info['file_name'] = f"{article_info['Date']}_{article_info['Organization']}_{article_info['Title']}.pdf"
        
        return article_info

    def download_pdf(self, article_info):
        pass
    
def main(date_from, headless=False, overwrite=False):
    try:
        date_from = datetime.strptime(date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        return

    articles_index_df = pd.DataFrame(S3MacroManager().get_articles_index())

    scraper = MyScraper(date_from, headless=headless)
    new_articles = scraper.process_articles(articles_index_df, date_from, overwrite)
    scraper.store_articles(new_articles)
    
    logger.info(f"Completed with {len(new_articles)} new articles.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape BIS articles')
    parser.add_argument("-df", "--date_from", type=str, required=True, help='Date (%Y-%m-%d) to scrape back from')
    parser.add_argument("--overwrite", action='store_true', help="Re-generate if summary exists (default: False)")
    parser.add_argument("--headless", action='store_true', help="Run browser in headless mode (default: False)")
    args = parser.parse_args()

    main(args.date_from, headless=args.headless, overwrite=args.overwrite)
