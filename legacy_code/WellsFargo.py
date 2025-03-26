import requests
from bs4 import BeautifulSoup
from utils import S3FileManager, days_between, append_article_to_json, clean_text, update_all_articles, \
    sanitize_filename, parse_text_from_pdf, rename_latest_file
from scrapers.llm_functions import clean_article
import argparse
import os
import colorlog
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import base64

# Constants
BASE_URL = 'https://www.wellsfargoadvisors.com'
stockMarketNews_url = '/research-analysis/commentary/stock-market-news.htm'
bondMarketCommentary_url = '/research-analysis/commentary/bond-market-commentary.htm'
marketCommentary_url = '/research-analysis/commentary/stock-market-commentary.htm'
lookingAhead_url = '/research-analysis/commentary/looking-ahead.htm'
investmentStrategy_url = '/research-analysis/strategy/weekly.htm'
chartOfWeek_url = '/research-analysis/strategy/chart-of-week.htm'

# MidyearOutlook is a special report and only need to download once
MidyearOutlook_url = 'https://saf.wellsfargoadvisors.com/emx/dctm/Research/wfii/wfii_reports/Investment_Strategy/outlook_report.pdf'

DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
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

# logger Setup
logger = colorlog.getLogger('JsafraSarasin')
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

def upload_aws(article_info, file_name):
    S3FileManager().store_file(db_name='macro_pdfs', date=article_info['Date'], file_name=file_name)
    append_article_to_json(article_info)
    update_all_articles(article_info)
    context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ article_info['file_name'])
    clean_context = clean_article(context)
    article_info.update(clean_context)
    S3FileManager().store_file('macro', data=article_info)

def scrape_MidyearOutlook():
    pdf_response = requests.get(MidyearOutlook_url, headers)
    if pdf_response.status_code == 200:
        if not os.path.exists('tmp'):
            os.makedirs('tmp')
        # Check write permissions
        if os.access('tmp', os.W_OK):
            with open(os.path.join('tmp', '2024-06-01_WellsFargo_2024 Midyear Outlook.pdf'), 'wb') as f:
                f.write(pdf_response.content)
    article_info = {
                    'Organization': 'WellsFargo',
                    'Date': '2024-06-01',
                    'Title': '2024 Midyear Outlook',
                    'Link': MidyearOutlook_url,
                    'Description': 'approaching the economy pivot point',
                }
    upload_aws(article_info, '2024-06-01_WellsFargo_2024 Midyear Outlook.pdf')

def scrape_articles(url, driver, article_title):
    url = BASE_URL + url
    today = datetime.date.today()
    article_date = today.strftime('%Y-%m-%d')
    article_info = {
                    'Organization': 'WellsFargo',
                    'Date': article_date,
                    'Title': article_title,
                    'Link': url,
                    'Description': '',
                }
    logger.info(f'Article info:\n{article_info}\n')
    file_name = sanitize_filename(f"{article_date}_WellsFargo_{article_title}.pdf")
    driver.get(url)
    pdf_link = have_pdf_link(driver)
    if pdf_link:
        pdf_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href$='.pdf']"))
        )
        # Click the button
        pdf_button.click()
        rename_latest_file(DOWNLOAD_DIR, filename=file_name)
        logger.info(f"Downloaded PDF for {article_info['Title']}\n")
        upload_aws(article_info, file_name)
    else:
        # print the page
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
        logger.info(f"Downloaded PDF for {article_info['Title']}\n")
        upload_aws(article_info, file_name)

def have_pdf_link(driver):
    page_source = driver.execute_script("return document.documentElement.outerHTML;")
    soup = BeautifulSoup(page_source, 'html.parser')
    pdf_link = soup.find_all('a', href=lambda href: href and href.endswith('.pdf'))
    # print(f"Type of pdf_link: {type(pdf_link)}, Value: {pdf_link}")
    if pdf_link:
        return pdf_link[0]['href']
    else:
        return False

def scrape_WellsFargo(date_from):
    # scrape_MidyearOutlook()
    driver = webdriver.Chrome(options=options)
    scrape_articles(stockMarketNews_url, driver, 'Stock Market News')
    scrape_articles(bondMarketCommentary_url, driver, 'Bond Market Commentary')
    scrape_articles(marketCommentary_url, driver, 'Market Commentary')
    scrape_articles(lookingAhead_url, driver, 'Looking Ahead')
    scrape_articles(investmentStrategy_url, driver, 'Investment strategy')
    scrape_articles(chartOfWeek_url, driver, 'Chart of the week')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape WellsFargo articles')
    parser.add_argument("-df", "--date_from", type=str, help='Date (%Y-%m-%d) to scrape back')
    args = parser.parse_args()
    scrape_WellsFargo(args.date_from)