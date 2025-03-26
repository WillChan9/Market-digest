import argparse
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urljoin
import os
import colorlog

from scrapers.llm_functions import clean_article
from utils import S3FileManager, days_between, append_article_to_json, clean_text, sanitize_filename, \
    update_all_articles, parse_text_from_pdf

# Constants
DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
BASE_URL = 'https://www.morganstanley.com/'
URL = 'https://www.morganstanley.com/im/en-us/institutional-investor/insights.html'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7,zh-CN;q=0.6',
    'Referer': 'https://www.morganstanley.com/',
    'Sec-Fetch-Dest': 'script',
    'Sec-Fetch-Mode': 'no-cors',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'Sec-Ch-Ua-Mobile': '?1',
    'Sec-Ch-Ua-Platform': '"Android"',
}

# Configure logging
logger = colorlog.getLogger('MorganStanley')
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
# Add the handler to the logger
logger.addHandler(handler)


def fetch_articles():
    """Fetch articles from the Morgan Stanley website."""
    response = requests.get(URL, headers=HEADERS)
    if response.status_code == 200:
        return response.text
    else:
        logger.error(f"Failed to retrieve the webpage, status code: {response.status_code}")
        return None


def parse_articles(page_content, date_from):
    """Parse articles from the page content."""
    soup = BeautifulSoup(page_content, 'html.parser')
    articles = []
    article_entries = soup.find_all('div', class_='borderBottom borderBottomSm borderBottomXs noPadding noMargin row')

    for entry in article_entries:
        date_span = entry.find('span', class_='pressCenterDate')
        if not date_span:
            continue

        date_text = date_span.get_text(strip=True).replace('•\xa0', '').strip()
        try:
            article_date = datetime.strptime(date_text, '%b %d, %Y')
        except ValueError:
            logger.error(f"Date parsing error: {date_text}")
            continue

        if datetime.now() - timedelta(days=days_between(date_from)) <= article_date:
            title_h4 = entry.find('h4', class_='media-heading')
            author_div = entry.find('div', class_='insightAuthorName')
            abstract_div = entry.find('div', class_='pressCenterText')
            dt = article_date.strftime('%Y-%m-%d')
            title = clean_text(title_h4.get_text(strip=True)).replace(' ', '_') if title_h4 else 'No Title'
            link = title_h4.find('a')['href'] if title_h4 and title_h4.find('a') else 'No Link'
            article_info = {
                'Organization': 'MorganStanley',
                'Date': dt,
                'Title': title,
                'Author': clean_text(author_div.get_text(strip=True)) if author_div else 'No Author',
                'Description': clean_text(abstract_div.get_text(strip=True)) if abstract_div else 'No Abstract',
                'Link': BASE_URL+link,
            }
            articles.append(article_info)
            # update_all_articles(article_info)
            # S3FileManager().store_file('macro', data=article_info)

            logger.info(f'Article info:\n{article_info}\n\n')

    return articles


def download_pdf(article_info):
    """Download PDF from the article link."""
    article_title = article_info['Title']
    article_date = article_info['Date']
    article_link = article_info['Link']
    file_name = sanitize_filename(f"{article_date}_MorganStanley_{article_title}.pdf")
    article_info['file_name'] = file_name

    detail_response = requests.get(article_link)
    if detail_response.status_code == 200:
        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
        pdf_link = detail_soup.find('a', href=lambda x: x and x.startswith('/im/publication/insights/articles'))

        if pdf_link:
            pdf_url = urljoin(BASE_URL, pdf_link['href'])
            pdf_response = requests.get(pdf_url)
            if pdf_response.status_code == 200:
                save_pdf(pdf_response, article_info)
            else:
                logger.error(f"Failed to download the PDF for {article_title}, status code: {pdf_response.status_code}")
        else:
            logger.warning(f"No PDF download link found for {article_title}")
    else:
        logger.error(
            f"Failed to open the article's detail page for {article_title}, status code: {detail_response.status_code}")


def save_pdf(pdf_response, article_info):
    """Save PDF to a local directory and upload to S3."""
    file_name = article_info['file_name']
    os.makedirs('tmp', exist_ok=True)
    try:
        with open(os.path.join('tmp', file_name), 'wb') as f:
            f.write(pdf_response.content)
        logger.info(f"Downloaded PDF for {article_info['Title']}")
        S3FileManager().store_file(db_name='macro_pdfs', date=article_info['Date'], file_name=file_name)
        append_article_to_json(article_info)
        update_all_articles(article_info)
        context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ file_name)
        clean_context = clean_article(context)
        article_info.update(clean_context)
        S3FileManager().store_file('macro', data=article_info)
    except IOError:
        logger.error("Error, checking write permissions.")


def scrape_morgan_stanley(date_from):
    """Main scraping function for Morgan Stanley articles."""
    page_content = fetch_articles()
    if page_content:
        articles = parse_articles(page_content, date_from)
        for article in articles:
            download_pdf(article)
            logger.info('-' * 50)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape Morgan Stanley articles')
    parser.add_argument("-df", "--date_from", type=str, help='Date (%Y-%m-%d) to scrape back', required=True)
    args = parser.parse_args()
    scrape_morgan_stanley(args.date_from)
