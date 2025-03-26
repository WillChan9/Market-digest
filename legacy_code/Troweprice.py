import requests
from bs4 import BeautifulSoup
import logging
import os
from datetime import datetime, timedelta
from urllib.parse import urljoin
from utils import S3FileManager, days_between, append_article_to_json, clean_text, update_all_articles, \
    sanitize_filename, parse_text_from_pdf, rename_latest_file
from scrapers.llm_functions import clean_article
import argparse
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import base64
import time


DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
options = Options()
options.add_experimental_option('prefs', {
    "download.default_directory": DOWNLOAD_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "plugins.always_open_pdf_externally": True,
    "profile.default_content_settings.popups": 0,
    "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
})
url = 'https://www.troweprice.com/personal-investing/resources/insights/all-insights.html'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
base_url = 'https://www.troweprice.com/'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Troweprice')


def extract_date(text):
    # Define month mapping
    month_mapping = {
        "Jan": "01", "January": "01",
        "Feb": "02", "February": "02",
        "Mar": "03", "March": "03",
        "Apr": "04", "April": "04",
        "May": "05",
        "Jun": "06", "June": "06",
        "Jul": "07", "July": "07",
        "Aug": "08", "August": "08",
        "Sep": "09", "September": "09",
        "Oct": "10", "October": "10",
        "Nov": "11", "November": "11",
        "Dec": "12", "December": "12"
    }

    # There are several date formats on the website
    date_pattern = r'(?<!\d)(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s*)(\d{1,2})(?:,\s*(\d{4}))?'
    match = re.search(date_pattern, text, re.IGNORECASE)

    if match:
        month = match.group(1).capitalize()  # Capitalize month name for consistency
        day = match.group(2).zfill(2)  # Zero-pad day to ensure it is two digits
        year = match.group(3) if match.group(3) else "unknown year"

        if year != "unknown year":
            month_num = month_mapping.get(month)
            if month_num:
                return f"{year}-{month_num}-{day}"

    return None


def download_pdfs(article_info):
    article_url = article_info['Link']
    article_title = article_info['Title']
    article_date = article_info['Date']
    file_name = sanitize_filename(f"{article_date}_{article_info['Organization']}_{article_title}.pdf")
    article_info['file_name'] = file_name

    article_response = requests.get(article_url, headers)
    if article_response.status_code == 200:
        article_soup = BeautifulSoup(article_response.text, 'html.parser')
        pdf_link = article_soup.find(
            lambda tag: tag.name == "a" and "/content/dam/iinvestor/resources/insights/pdfs" in tag.get('href', ''))
        if pdf_link:
            article_info['Link'] = urljoin(base_url, pdf_link['href'])
            pdf_response = requests.get(article_info['Link'], headers)
            if pdf_response.status_code == 200:
                try:
                    # Check write permissions
                    if os.access('tmp', os.W_OK):
                        with open(os.path.join('tmp', file_name), 'wb') as f:
                            f.write(pdf_response.content)
                        logger.info(f"Downloaded PDF for {article_title}\n")
                        append_article_to_json(article_info)
                        update_all_articles(article_info)
                        context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+file_name)
                        clean_context = clean_article(context)
                        article_info.update(clean_context)
                        S3FileManager().store_file('macro', data=article_info)
                    else:
                        logger.error("No write permissions for the directory 'tmp'")
                except Exception as e:
                    logger.error(f"Error writing file: {e}")
        else:
            logger.warning(f"PDF link not found: {article_url}")
            # TODO: need to add a web driver page download function here
            driver = webdriver.Chrome(options=options)
            driver.get(article_url)
            time.sleep(2)
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
            S3FileManager().store_file(db_name='macro_pdfs', date=article_date, file_name=file_name)
            append_article_to_json(article_info)
            update_all_articles(article_info)
            context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ article_info['file_name'])
            clean_context = clean_article(context)
            article_info.update(clean_context)
            S3FileManager().store_file('macro', data=article_info)
            logger.info(f"Downloaded PDF for {article_title}\n")
    else:
        logger.error(f"Failed to fetch article page: {article_response.status_code}")


def scrape_Troweprice(date_from):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        content_boxes = soup.find_all(class_=['content-box-holder'])
        for box in content_boxes:
            date_spans = box.find_all('span', class_='size-14')
            date_span = date_spans[0]
            raw_date = date_span.get_text(strip=True)
            dt = extract_date(raw_date)
            if dt:  # when successfull extract date time
                if datetime.now() - timedelta(days=days_between(date_from)) <= datetime.strptime(dt, '%Y-%m-%d'):
                    seo_heading = box.find(class_='seo-heading')
                    title = seo_heading.get_text(strip=True)
                    paragraph_content = box.find(class_=['paragraph-md'])
                    if paragraph_content:
                        description_span = paragraph_content.find('span', class_='text-light')
                        if description_span:
                            article_description = description_span.get_text(strip=True)
                        else:
                            article_description = ''
                            logger.error("Description not found in this box.")
                    else:
                        article_description = ''
                        logger.error("Paragraph content not found in this box.")
                    link_tag = box.find('a', class_='content-box-link')
                    if link_tag and link_tag.has_attr('href'):
                        article_url = urljoin(base_url, link_tag['href'])
                        title = clean_text(title).replace(' ', '_')
                        article_info = {
                            'Organization': 'Troweprice',
                            'Date': dt,
                            'Title': title,
                            'Link': article_url,
                            'Description': clean_text(article_description),

                        }

                        logger.info(f'Article info:\n{article_info}\n\n')
                        download_pdfs(article_info)
                    else:
                        logger.error(f"Link not found in this box.")
            else:
                logger.error("Date not found or format error!")
    else:
        logger.error("Failed to retrieve the webpage. Status code:", response.status_code)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Troweprice articles')
    parser.add_argument("-df", "--date_from", type=str, help='Date (%Y-%m-%d) to scrape back')
    args = parser.parse_args()
    scrape_Troweprice(args.date_from)
