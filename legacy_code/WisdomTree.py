import requests
from bs4 import BeautifulSoup
import logging
import os
from datetime import datetime, timedelta
import argparse

from scrapers.llm_functions import clean_article
from utils import S3FileManager, days_between, append_article_to_json, clean_text, update_all_articles, \
    parse_text_from_pdf

# Constants
URL = "https://www.wisdomtree.com/investments/all-insights"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
}
BASE_URL = "https://www.wisdomtree.com"
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
# Configure logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('WisdomTree')


def sanitize_filename(filename):
    """Clean up the invalid symbols in file name."""
    illegal_characters = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in illegal_characters:
        filename = filename.replace(char, '')
    return filename


def fetch_page_content(url, headers):
    """Fetch the content of the page."""
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        logger.error(f"Failed to retrieve the webpage, status code: {response.status_code}")
        return None


def fetch_json_data(api_url, headers):
    """Fetch JSON data from the API."""
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to retrieve JSON data, status code: {response.status_code}")
        return None


def extract_pdf_link(soup):
    """Extract the PDF link from the article soup."""
    pdf_api = soup.find_all('a', href=True)
    for link in pdf_api:
        href = link['href']
        if '/api/sitecore/pdf' in href:
            return BASE_URL + href
    return None


def download_pdf(pdf_url, article_info):
    """Download PDF from the given URL."""
    pdf_response = requests.get(pdf_url, headers=HEADERS)
    if pdf_response.status_code == 200:
        save_pdf(pdf_response, article_info)
    else:
        logger.error(f"Failed to download PDF, status code: {pdf_response.status_code}")


def save_pdf(pdf_response, article_info):
    """Save the PDF to a local directory and upload to S3."""
    try:
        file_name = sanitize_filename(f"{article_info['Date']}_WisdomTree_{article_info['Title']}.pdf")
        article_info['file_name'] = file_name
        with open(os.path.join('tmp', file_name), 'wb') as f:
            f.write(pdf_response.content)
        logger.info(f"Downloaded PDF for {article_info['Title']}")
        db = S3FileManager()
        db.store_file(db_name='macro_pdfs', date=article_info['Date'], file_name=file_name)
        append_article_to_json(article_info)
        update_all_articles(article_info)
        context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ article_info['file_name'])
        clean_context = clean_article(context)
        article_info.update(clean_context)
        S3FileManager().store_file('macro', data=article_info)
    except IOError:
        logger.error("Error, checking write permissions.")


def scrape_wisdomtree(date_from):
    """Main scraping function for WisdomTree articles."""
    page_content = fetch_page_content(URL, HEADERS)
    if not page_content:
        return

    soup = BeautifulSoup(page_content, 'html.parser')
    data_item_id_element = soup.find(attrs={"data-item-id": True})
    data_item_id = data_item_id_element['data-item-id'].strip('{}') if data_item_id_element else None

    if not data_item_id:
        logger.error("Failed to extract data-item-id from the page.")
        return

    api_url = f"https://www.wisdomtree.com/api/sitecore/insightsapi/getinsights?itemId=%7B{data_item_id}%7D&virtualFolder=%2Finvestments%2F&categoryid=&authorId=&tagName=&searchText=&startRowIndex=0&maximumRows=20"
    json_data = fetch_json_data(api_url, HEADERS)
    if not json_data:
        return

    for article in json_data:
        try:
            if datetime.now() - timedelta(days=days_between(date_from)) <= datetime.strptime(article["PublishDate"],
                                                                                             "%m/%d/%Y"):
                dt = datetime.strptime(article["PublishDate"], "%m/%d/%Y").strftime('%Y-%m-%d')
                title = clean_text(article["Title"]).replace(' ', '_')
                article_info = {
                    'Organization': 'WisdomTree',
                    'Date': dt,
                    'Title': title,
                    'Link': article["PostUrl"],
                    'Description': clean_text(article["Description"]),
                }
                article_content = fetch_page_content(article_info['Link'], HEADERS)
                if article_content:
                    article_soup = BeautifulSoup(article_content, 'html.parser')
                    pdf_url = extract_pdf_link(article_soup)
                    if pdf_url:
                        download_pdf(pdf_url, article_info)
                    else:
                        logger.warning(f"No PDF link found for {article_info['Title']}")
                logger.info(f'Article info:\n{article_info}\n')
        except ValueError:
            logger.error(f"Skipping article with invalid date format: {article['Title']}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape WisdomTree articles')
    parser.add_argument("-df", "--date_from", type=str, help='Date (%Y-%m-%d) to scrape back', required=True)
    args = parser.parse_args()
    scrape_wisdomtree(args.date_from)
