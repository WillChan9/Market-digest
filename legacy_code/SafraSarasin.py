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
BASE_URL = 'https://jsafrasarasin.com'
LOG_FILE = 'JsafraSarasin.log'
URL = 'https://jsafrasarasin.com/content/jsafrasarasin/language-masters/en/our-perspectives.html'

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
logger = setup_logging('SafraSarasin', level=logging.INFO)

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


def fetch_pagesource(driver):
    driver.get(URL)
    time.sleep(4)
    try:
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH,
                                        "//div[contains(@class, 'allCookies') and .//span[contains(@class, 'title')][contains(., 'ACCEPT ALL')]]"))
        ).click()
        input_field = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".autocomplete input[name='input__0']"))
        )
        input_field.click()
        input_field.send_keys("Switzerland")
        WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "input__0"))
        )
        driver.find_element(By.ID, "input__0").click()
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.jss-cmplf__btn"))
        ).click()
        time.sleep(7)
        page_source = driver.execute_script("return document.documentElement.outerHTML;")
    except:
        logger.error("Data fetching error! Check the webdriver.\n")
    return page_source


def download_pdf(driver, article_info):
    article_title = article_info['Title']
    article_date = article_info['Date']
    file_name = sanitize_filename(f"{article_date}_SafraSarasin_{article_title}.pdf")

    pdf_path = os.path.join(DOWNLOAD_DIR, file_name)

    driver.execute_script("window.open('');")
    driver.switch_to.window(driver.window_handles[1])
    driver.get(article_info['Link'])
    time.sleep(2)  # wait till loading finish

    try:  # find the pdf link
        link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "a.jss-btn__link[href*='https://publications.jsafrasarasin.com']"))
        )
        link.click()
        time.sleep(6)  # wait till download finish

        rename_latest_file(DOWNLOAD_DIR, filename=file_name)
        driver.close()
        driver.switch_to.window(driver.window_handles[0])
        logger.info(f"Downloaded PDF for {article_info['Title']}")
        return file_name 
        
    except:  # otherwise print the page
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


def scrape_JsafraSarasin(date_from, articles_index_df, overwrite):
    articles = []
    driver = webdriver.Chrome(options=options)
    source_code = fetch_pagesource(driver)

    soup = BeautifulSoup(source_code, 'html.parser')
    items = soup.find_all('div', class_='jss-cHub--card white')
    for item in items:
        raw_date = item.find('h4', class_='jss-cHub--card__info--pubDate').text
        article_date = datetime.strptime(raw_date, "%d.%m.%Y").strftime("%Y-%m-%d")

        if date_from > article_date:
            continue
                
        article_title = item.find('h2', class_='jss-cHub--card__title').text
        article_description = item.find('p', class_='jss-cHub--card__desc').text
        article_link = item.find('a', class_='jss-cHub--card__link')['href']
        article_link = urljoin(BASE_URL, article_link)
        article_info = {
            'Organization': 'JsafraSarasin',
            'Date': article_date,
            'Title': article_title,
            'Link': article_link,
            'Description': clean_text(article_description),
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

    driver.close()
    return articles


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Safra Sarasin articles')
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
    articles = scrape_JsafraSarasin(date_from, articles_index_df, args.overwrite)

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