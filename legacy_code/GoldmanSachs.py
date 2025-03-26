import os
import time
import logging
import base64
from datetime import datetime, timedelta
from urllib.parse import urljoin
import argparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import requests
import colorlog

from scrapers.llm_functions import clean_article
from utils import S3MacroManager, days_between, append_article_to_json, sanitize_filename, rename_latest_file, update_all_articles, parse_text_from_pdf

# Constants
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
BASE_URL = 'https://am.gs.com'
API_URL = 'https://am.gs.com/services/search-engine/en-us/institutions/search/insights?hitsPerPage=10&tags=product%2Fmacroeconomics'
LOG_FILE = 'GoldmanSachs.log'

# Setup
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
# logger Setup
logger = colorlog.getLogger('GoldmanSachs')
logger.setLevel(logging.INFO)
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white'
    }))
logger.addHandler(handler)

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

def upload_aws(article_info, file_name):

    # append_article_to_json(article_info)
    # update_all_articles(article_info)
    context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ article_info['file_name'])
    clean_context = clean_article(context)
    article_info.update(clean_context)
    print(article_info)
    # s3 = S3MacroManager()
    # s3.store_pdf(date=article_info['Date'], file_name=file_name)
    # s3().store_file('macro', data=article_info)

def download_pdfs(driver, article_info):
    """Download PDFs and store them using S3FileManager."""
    driver.get(article_info['Link'])
    time.sleep(2)
    page_source = driver.execute_script("return document.documentElement.outerHTML;")
    soup = BeautifulSoup(page_source, 'html.parser')
    span_elements = soup.find_all('span', string='Download')
    file_name = sanitize_filename(f"{article_info['Date']}_GoldmanSachs_{article_info['Title']}.pdf")
    if span_elements:
        download_button = driver.find_element(By.XPATH, "//span[text()='Download']")
        download_button.click()
        time.sleep(5)
        rename_latest_file(DOWNLOAD_DIR, file_name)
        logger.info(f"Downloaded PDF for {article_info['Title']}\n")
        upload_aws(article_info, file_name)
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
            rename_latest_file(DOWNLOAD_DIR, file_name)
            logger.info(f"Downloaded PDF for '{article_info['Title']}'\n")
            upload_aws(article_info, file_name)
        except Exception as e:
            logger.error(f"Failed to save PDF for '{article_info['Title']}': {e}")


def scrape_goldmansachs(date_from):
    """Scrape GoldmanSachs articles. First use api to get article information, 
    then use browser to download the pdf."""
    driver = webdriver.Chrome(options=options)
    driver.get('https://am.gs.com/en-us/institutions/insights/topics/macroeconomics')
    institution_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.ID, "button-select-institutions"))
        )
    institution_button.click()
    response = requests.get(API_URL)
    if response.status_code == 200:
        data = response.json()
        for article in data['insights']['hits']:
            parsed_datetime = datetime.fromisoformat(article['publishDate'].replace('Z', '+00:00'))
            formatted_date = parsed_datetime.strftime('%Y-%m-%d')
            if datetime.now() - timedelta(days=days_between(date_from)) <= datetime.strptime(formatted_date, '%Y-%m-%d'):
                article_info = {
                        'Organization': 'GoldmanSachs',
                        'Date': formatted_date,
                        'Title': article['title'],
                        'Link': BASE_URL+article['slug'],
                        'Description': article.get('summaryTeaserText', '')
                    }
                logger.info(f'Article info:\n{article_info}\n')
                download_pdfs(driver, article_info)
    else:
        logger.error("Fail to fetch the website api of Goldman Sachs")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape GoldmanSachs articles')
    parser.add_argument("-df", "--date_from", type=str, help='Date (%Y-%m-%d) to scrape back', required=True)
    args = parser.parse_args()

    scrape_goldmansachs(args.date_from)
