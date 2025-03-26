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

from utils import setup_logging, clean_text, sanitize_filename, rename_latest_file, get_content_and_summary
from macro_handler import S3MacroManager

import pandas as pd 

# Constants
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
BASE_URL = 'https://www.blackrock.com/'
ARTICLE_URL = "https://www.blackrock.com/corporate/insights/blackrock-investment-institute/archives#weekly-commentary"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('BlackRock', level=logging.INFO)

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
    """Download PDFs """
    pdf_link = article_info['Link']
    article_title = article_info['Title']
    article_date = article_info['Date']
    file_name = sanitize_filename(f"{article_date}_BlackRock_{article_title}.pdf")

    if pdf_link:
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(pdf_link)
        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) == 2)
        time.sleep(5)
        rename_latest_file(DOWNLOAD_DIR, filename=file_name)

        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        logger.info(f"Downloaded PDF for {article_title}")
        return file_name
    else:
        logger.warning(f"No PDF download link found for {article_title}")


def scrape_blackrock(date_from, articles_index_df, overwrite):
    """Scrape BlackRock articles."""

    articles = []


    driver = webdriver.Chrome(options=options)

    try:
        driver.get(ARTICLE_URL)
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept all")]'))).click()
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Accept")]'))).click()

        load_more_button = driver.find_element(By.XPATH, '//a[contains(@class, "load-more")]')
        load_more_button.click()

        source_code = driver.execute_script("return document.documentElement.outerHTML;")
        soup = BeautifulSoup(source_code, 'html.parser')

        items = soup.find_all('div', class_='item', style=lambda value: 'display: block' in value if value else False)
        for item in items:
            date_element = item.find('div', class_='attribution')
            date_text = date_element.get_text(strip=True) if date_element else ''
            try:
                article_date = datetime.strptime(date_text, "%b %d, %Y")
                if date_from > article_date.strftime("%Y-%m-%d"):
                    continue
            except ValueError:
                logger.error(f"Date format error: {date_text}")
                continue

            title_element = item.find('h2', class_='title')
            description_element = item.find('div', class_='description')
            pdf_link_element = title_element.find('a') if title_element else None
            pdf_link = urljoin(BASE_URL, pdf_link_element['href']) if pdf_link_element and pdf_link_element.has_attr(
                'href') else ''
            title = clean_text(title_element.get_text(strip=True)) if title_element else ''
            description = clean_text(description_element.get_text(strip=True)) if description_element else ''

            article_info = {
                'Organization': 'BlackRock',
                'Date': article_date.strftime('%Y-%m-%d'),
                'Title': title.replace(' ', '_'),
                'Link': pdf_link,
                'Description': description,
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

    finally:
        driver.quit()
    
    return articles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape BlackRock articles')
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
    articles = scrape_blackrock(date_from, articles_index_df, args.overwrite)

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