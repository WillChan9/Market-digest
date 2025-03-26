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

from utils import setup_logging, sanitize_filename, rename_latest_file, get_content_and_summary
from macro_handler import S3MacroManager

import pandas as pd 
import base64, requests

# Constants
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
BASE_URL = 'https://www.ecb.europa.eu/'
LOG_FILE = 'EU_bank.log'
URL = 'https://www.ecb.europa.eu/press/pubbydate/html/index.en.html'

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('EUCentralBank', level=logging.INFO)

# Configure WebDriver options
options = Options()
options.add_experimental_option('prefs', {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True,
    "profile.default_content_settings.popups": 0,
    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
})

def download_pdf(driver, article_info):
    article_title = article_info['Title']
    article_date = article_info['Date']
    file_name = sanitize_filename(f"{article_date}_EUCentralBank_{article_title}.pdf")

    if article_info['Link'].endswith('.html'):
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[1])
        driver.get(article_info['Link'])
        time.sleep(2)  # wait till loading finish
        result = driver.execute_cdp_cmd("Page.printToPDF", {
            "landscape": False,
            "displayHeaderFooter": False,
            "printBackground": True,
            "preferCSSPageSize": True,
        })
        pdf_base64 = result['data']
        with open('tmp/data.pdf', 'wb') as f:
            f.write(base64.b64decode(pdf_base64))
        rename_latest_file(DOWNLOAD_DIR, file_name)
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        logger.info(f"Downloaded PDF for {article_title}\n")
        return file_name
    elif article_info['Link'].endswith('.pdf'):
        pdf_response = requests.get(article_info['Link'])
        if pdf_response.status_code == 200:
            pdf_path = os.path.join('tmp', file_name)
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
            logger.info(f"Downloaded PDF for {article_info['Title']}\n")
            return file_name


def scrape_EUBank(date_from, articles_index_df, overwrite ):
    articles = []

    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    time.sleep(5)
    for _ in range(10):
        driver.execute_script("window.scrollBy(0, 2000);")  # Scroll down by 500 pixels
        time.sleep(1)  # Wait for one second
    page_source = driver.execute_script("return document.documentElement.outerHTML;")
    soup = BeautifulSoup(page_source, 'html.parser')
    sort_wrapper = soup.find('div', class_='sort-wrapper')
    dl_tag = sort_wrapper.find('dl')
    if dl_tag:
        dates = dl_tag.find_all('dt', recursive=False)
        details = dl_tag.find_all('dd', recursive=False)
        for date, detail in zip(dates, details):
            raw_date = date.text.strip()  # Extract text from the date

            article_date = datetime.strptime(raw_date, "%d %B %Y").strftime("%Y-%m-%d")
            if date_from > article_date:
                continue

            title_div = detail.find('div', class_='title')  # Find the title div within details
            link = title_div.find('a')  # Find the link within the title div
            article_link = urljoin(BASE_URL, link['href'])
            article_title = link.text.strip()
            article_info = {
                'Organization': 'EU Central Bank',
                'Date': article_date,
                'Title': article_title,
                'Link': article_link,
                'Description': '',
            }
            file_name = download_pdf(driver, article_info)
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
                time.sleep(1)

    return articles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape EU Central Bank articles')
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
    articles_index = pd.DataFrame(s3.get_articles_index())

    # Scrape articles starting from the given date
    articles = scrape_EUBank( date_from, articles_index, args.overwrite )

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