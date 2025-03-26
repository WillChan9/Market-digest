import os
import time
import logging
from urllib.parse import urljoin
from datetime import datetime, timedelta
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from utils import setup_logging, clean_text, sanitize_filename, get_content_and_summary, rename_latest_file
from macro_handler import S3MacroManager

import pandas as pd 
import requests, base64

# Constants
URL = 'https://www.ml.com/capital-market-outlook/_jcr_content/bulletin-tilespattern.pagination.recent.json/1.html?_=1716077499'
BASE_URL = 'https://www.ml.com'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
}
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')

# Setup
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('LombardOdier', level=logging.INFO)


def fetch_articles():
    """Fetch articles from the Merrill Lynch website."""
    response = requests.get(URL, headers=HEADERS)
    if response.status_code == 200:
        return response.json().get('pages', [])
    else:
        logger.error(f"Failed to retrieve the page. Status code: {response.status_code}")
        return []




        # article_response = requests.get(article_url, headers=HEADERS)
        # if article_response.status_code == 200:
        #     article_soup = BeautifulSoup(article_response.content, 'html.parser')
        #     pdf_link_tag = article_soup.find('a', class_='t-track-teaser-cta')
        #     if pdf_link_tag:
        #         return BASE_URL + pdf_link_tag['href']
        #     else:
        #         return None
        # else:
        #     logger.error(f"Failed to retrieve the article. Status code: {article_response.status_code}")
        #     return None


        # if pdf_link:
        #     download_pdf(article_info)
        # else:
        #     logger.error("No PDF link found")

def download_pdf(article_info):

    file_name = sanitize_filename(f"{article_info['Date']}_Merrill_{article_info['Title']}.pdf")
    article_info['file_name'] = file_name
    pdf_link = article_info['Link']
    pdf_response = requests.get(pdf_link, headers=HEADERS)
    print('--------------------------')
    print(article_info)
    asdf
    if pdf_response.status_code == 200:
        pdf_path = os.path.join('tmp', file_name)
        with open(pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        logger.info(f"Downloaded PDF for {article_info['Title']}")
        return file_name

    else:
        logger.error(f"Failed to download PDF. Status code: {pdf_response.status_code}")


def scrape_merrill(date_from, articles_index_df, overwrite):
    """Main scraping function for Merrill Lynch articles."""
    articles = fetch_articles()

    for article in articles:
        parsed_date = BeautifulSoup(article.get('author', ''), 'html.parser').text.strip()
        article_date = datetime.strptime(parsed_date, "%B %d, %Y").strftime("%Y-%m-%d")
        if date_from > article_date:
            continue

        title = article.get('title', 'No Title')
        path = article.get('path', 'No Path')
        description = BeautifulSoup(article.get('subtitle', ''), 'html.parser').text
        article_url = f"{BASE_URL}/{path}.recent.html"

        title = clean_text(title).replace(' ', '_')
        article_info = {
            'Organization': 'Merrill',
            'Date': article_date,
            'Title': title,
            'Link': article_url,
            'Description': clean_text(description)
        }

        file_name = download_pdf( article_info)
        article_info['file_name'] = file_name
        logger.info(f"Article info:{article_info['Title']}")
        # Check if the title or file name exists
        existing_records = articles_index_df[
            (articles_index_df['Title'].str.lower() == article_info['Title'].lower()) |
            (articles_index_df['file_name'].str.lower() == file_name.lower())
        ]
        if not existing_records.empty and overwrite == False:
            logger.warning(f"File {article_info['Title']} already exists, pass")
            continue

        # Get content and summary 
        clean_content = get_content_and_summary(file_name)
        if clean_content:
            article_info.update(clean_content)
            articles.append( article_info )
            time.sleep(2)

    return articles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Merill articles')
    parser.add_argument("-df", "--date_from", type=str, required=True, help='Date (%Y-%m-%d) to scrape back')
    parser.add_argument("-o", "--overwrite", type=bool, help="Re-generate if summary exists", default=False)    
    args = parser.parse_args()

    # Parse date to ensure it's in correct format
    try:
        date_from = datetime.strptime(args.date_from, '%Y-%m-%d').strftime("%Y-%m-%d")
    except ValueError:
        logger.error("Incorrect date format, should be YYYY-MM-DD")
        exit(1)

    s3 = S3MacroManager()
    articles_index_df = pd.DataFrame(s3.get_articles_index())

    # Scrape articles starting from the given date
    articles = scrape_merrill(date_from, articles_index_df, args.overwrite)

    # Handle the case where no articles were found
    if not articles:
        logger.info("No articles found for the given date range.")
        exit(0)

    # List to hold articles that need to be appended
    new_articles = []

    for article in articles:
        s3.store_pdf(article['Date'], article['file_name'])
        s3.store_json(article)
        new_articles.append(article)  # Add to the list of new articles

    # Append only new articles to the index
    if new_articles:
        s3.append_articles_to_index(new_articles)
    else:
        logger.info("No new articles to append")