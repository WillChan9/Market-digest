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
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
BASE_URL = 'https://www.lombardodier.com'
URL = 'https://www.lombardodier.com/home/about-us/insights.html'

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('LombardOdier', level=logging.INFO)

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
    """Download PDFs and store them using S3FileManager."""
    driver.get(article_info['Link'])
    time.sleep(2)
    page_source = driver.execute_script("return document.documentElement.outerHTML;")
    soup = BeautifulSoup(page_source, 'html.parser')
    pdf_links = soup.find_all('a', href=lambda href: href and href.endswith('.pdf'))
    
    file_name = sanitize_filename(f"{article_info['Date']}_LombardOdier_{article_info['Title']}.pdf")
    pdf_path = os.path.join(DOWNLOAD_DIR, file_name)

    if pdf_links:
        pdf_response = requests.get(BASE_URL + pdf_links[0]['href'])
        if pdf_response.status_code == 200:
            
            with open(pdf_path, 'wb') as f:
                f.write(pdf_response.content)
        logger.info(f"Downloaded PDF for {article_info['Title']}\n")
        return file_name
    else:
        try:
            time.sleep(3)
            result = driver.execute_cdp_cmd("Page.printToPDF", {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "preferCSSPageSize": True,
            })
            pdf_base64 = result['data']
            with open('tmp/data.pdf', 'wb') as f:
                f.write(base64.b64decode(pdf_base64))
            os.rename( f"{DOWNLOAD_DIR}/data.pdf", pdf_path )
            logger.info(f"HTML page printed to PDF for {article_info['Title']}")
            return file_name 
        except Exception as e:
            logger.error(f"Failed to save PDF for '{article_info['Title']}': {e}")


def scrape_lombardodier(date_from, articles_index_df, overwrite):
    articles = []

    driver = webdriver.Chrome(options=options)
    driver.get(URL)
    cookie_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "accept"))
        )
    cookie_button.click()
    for _ in range(7):
        driver.execute_script("window.scrollBy(0, 2000);")  # Scroll down by 500 pixels
        time.sleep(1)  # Wait for one second
    page_source = driver.execute_script("return document.documentElement.outerHTML;")
    soup = BeautifulSoup(page_source, 'html.parser')
    article_blocks = soup.find_all('div', class_ = 'overviewbloc js-item col-12 col-md-12 col-lg-8')
    for article in article_blocks:
        raw_date = article.find("time", class_="overviewbloc-date").get_text(strip=True)
        article_date = datetime.strptime(raw_date, "%B %d, %Y").strftime("%Y-%m-%d")
        if date_from > article_date:
            continue

        article_link = article.find("a", href=True)["href"]
        article_title = article.find("h3", class_="overviewbloc-title").get_text(strip=True)
        article_info = {
                    'Organization': 'LombardOdier',
                    'Date': article_date,
                    'Title': article_title,
                    'Link': BASE_URL + article_link,
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
            time.sleep(2)

    return articles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Lombard Odier articles')
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
    articles = scrape_lombardodier(date_from, articles_index_df, args.overwrite)

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