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

from utils import setup_logging, clean_text, sanitize_filename, get_content_and_summary
from macro_handler import S3MacroManager

import pandas as pd 
import requests, base64
# Constants
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
JPM_URL = "https://www.jpmorgan.com/services/json/v1/dynamic-grid.service/parent=jpmorgan/global/US/en/insights&comp=root/content-parsys/dynamic_grid&page=p1.json"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
}

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('JPMorgan', level=logging.INFO)

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
    file_name = sanitize_filename(f"{article_date}_JPMorgan_{article_title}.pdf")

    pdf_path = os.path.join(DOWNLOAD_DIR, file_name)

    if article_info["Link"].lower().endswith('.pdf'):
        response = requests.get(article_info["Link"])
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"Downloaded PDF for {article_info['Title']}")
        return file_name            
    else:
        try:
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[1])
            driver.get(article_info['Link'])
            time.sleep(5) 
            result = driver.execute_cdp_cmd("Page.printToPDF", {
                "landscape": False,
                "displayHeaderFooter": False,
                "printBackground": True,
                "preferCSSPageSize": True,
            })
            pdf_base64 = result['data']
            with open('tmp/data.pdf', 'wb') as f:
                f.write(base64.b64decode(pdf_base64))
            os.rename(f"{DOWNLOAD_DIR}/data.pdf", pdf_path)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])        
            logger.info(f"HTML page printed to PDF for {article_info['Title']}")
            return file_name 
        
        except Exception as e:
            logger.error(f"Failed to save PDF '{pdf_path}': {e}")

def scrape_jpmorgan(date_from, articles_index_df, overwrite=False):
    """ Main scraping function for JPMorgan articles """
    articles = []

    response = requests.get(JPM_URL, headers=HEADERS)
    if response.status_code == 200:
        driver = webdriver.Chrome(options=options)

        items =  response.json().get("items", [])
        for item in items:
            if "date" not in item:
                continue

            article_date = datetime.strptime(item["date"], "%B %d, %Y").strftime("%Y-%m-%d")
            if date_from > article_date:
                continue
            
            link = item["link"]
            description = item.get("description", "")
            title = clean_text(item["title"]).replace(' ', '_')
            if not link.startswith('https://www.'):
                link = urljoin('https://www.jpmorgan.com', link)
            article_info = {
                'Organization': 'JPMorgan',
                'Date': article_date,
                'Title': title,
                'Link': link,
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
                time.sleep(2)
        driver.quit()
        return articles
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape JPMorgan articles')
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
    articles = scrape_jpmorgan(date_from, articles_index_df, args.overwrite)

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