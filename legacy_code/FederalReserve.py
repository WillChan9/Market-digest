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

from utils import setup_logging, sanitize_filename, get_content_and_summary
from macro_handler import S3MacroManager

import pandas as pd 
import base64, requests
import re

#Constants
url = 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
base_url = 'https://www.federalreserve.gov/'
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('FederalReserve', level=logging.INFO)


def download_pdf(article_info):
    file_name = sanitize_filename( f"{article_info['Date']}_FederalReserve_{article_info['Title']}.pdf" )

    headers = {'User-Agent': 'Mozilla/5.0'}
    pdf_response = requests.get(article_info['Link'], headers=headers)
    
    if pdf_response.status_code == 200:
            pdf_path = os.path.join('tmp', file_name)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
            logger.info(f"Downloaded PDF for {article_info['Title']}")
            return file_name


def parse_month(month_str, year):
    # the month format are different: April, Apr/May, etc
    try:
        month_datetime = datetime.strptime(month_str, '%B')
    except ValueError:
        month_datetime = datetime.strptime(month_str, '%b')
    month_datetime = month_datetime.replace(year=year)
    return month_datetime


def scrape_FederalReserve(date_from, articles_index_df, overwrite):
    articles = []

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        panel = soup.find('div', class_='panel panel-default')
        meetings = panel.find_all('div', class_=lambda
            class_attr: class_attr and 'row' in class_attr.split() and 'fomc-meeting' in class_attr.split())
        # Get FOMC statements first
        for meeting in meetings:
            pdf_links = [a['href'] for a in meeting.find_all('a', string='PDF') if 'monetary' in a['href'] ]
            try:
                date_str = pdf_links[0].split('/')[-1].split('monetary')[1][:8]
                article_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")
            except:
                continue 

            if date_from > article_date:
                continue

            article_info = {
                'Organization': 'Federal Reserve',
                'Date': article_date,
                'Title': 'Federal Reserve Press Release',
                'Link': f"https://www.federalreserve.gov{pdf_links[0]}",
                'Description': f'Federal Reserve Press Release for {article_date} meeting'
            }

            file_name = download_pdf( article_info )
            article_info['file_name'] = file_name

            logger.info(f"Article info:{article_info['Title']}")
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
                time.sleep(1)

        # Get FOMC minutes second
        for meeting in meetings:
            pdf_links = [a['href'] for a  in meeting.find_all('a', string='PDF') if 'fomcminutes' in a['href'] ]
            element = meeting.find('div', class_=['fomc-meeting__minutes']).text
            date_match = re.search(r"(\w+\s\d{1,2},\s\d{4})", element)
            try:
                date_str = date_match.group(1)
                article_date = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except:
                continue
            
            meeting_date_str = pdf_links[0].split('/')[-1].split('fomcminutes')[1][:8]
            meeting_date = datetime.strptime(meeting_date_str, "%Y%m%d").strftime("%Y-%m-%d")

            article_info = {
                'Organization': 'Federal Reserve',
                'Date': article_date,
                'Title': 'Federal Reserve Minutes',
                'Link': f"https://www.federalreserve.gov{pdf_links[0]}",
                'Description': f"Federal Reserve Minutes for {meeting_date} meeting released on {article_date}"
            }            
            file_name = download_pdf( article_info )
            article_info['file_name'] = file_name

            logger.info(f"Article info:{article_info['Title']}")
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
                time.sleep(1)            
        return articles
    else:
        logger.error('Failed to retrieve the webpage')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Federal Reserve articles')
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
    articles = scrape_FederalReserve(date_from, articles_index_df, args.overwrite)

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