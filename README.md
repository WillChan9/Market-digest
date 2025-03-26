
# Market Digest

## Overview

Market Digest is a project designed to scrape macro reports from major financial institutions and generate macroeconomic summaries. It supports scraping from a variety of sources like BlackRock, Goldman Sachs, J.P. Morgan, Morgan Stanley, and others. The reports are processed and stored, allowing users to analyze and digest market information efficiently.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Environment Setup](#environment-setup)
- [Scraper Setup](#scraper-setup)
- [Usage](#usage)
  - [Running Individual Scrapers](#running-individual-scrapers)
  - [Running All Scrapers](#running-all-scrapers)
  - [Checking Output in S3](#checking-output-in-s3)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Data Storage Information](#data-storage-information)
- [S3MacroManager Class Overview](#s3macromanager-class-overview)
- [Scraper Framework Overview](#scraper-framework-overview)
  - [How to Create a New Scraper](#how-to-create-a-new-scraper)

## Installation

Make sure to have Python 3.11 installed and properly configured on your machine. Follow these steps to install the necessary dependencies.

### Initial Environment Setup

1. Install Python (3.11):

    ```bash
    sudo apt update
    sudo apt install build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev         libsqlite3-dev libreadline-dev libffi-dev curl libbz2-dev
    wget https://www.python.org/ftp/python/3.11.9/Python-3.11.9.tgz
    tar -xzf Python-3.11.9.tgz
    cd Python-3.11.9
    ./configure --enable-optimizations
    make -j 2
    sudo make install
    ```

2. Install Poetry for managing dependencies:

    ```bash
    pip install poetry
    ```

3. Use Python 3.11 in Poetry:

    ```bash
    poetry env use python3.11
    ```

### Environment Setup (before each use)

Activate your virtual environment and install dependencies:

```bash
source activate macro  # For Linux
poetry shell  # Activate the Poetry environment
poetry install --no-root  # Install dependencies
```

## Scraper Setup

Install Google Chrome and ChromeDriver to run the scrapers in headless mode:

1. Install Google Chrome:

    ```bash
    wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo apt install ./google-chrome-stable_current_amd64.deb
    ```

2. Install ChromeDriver:

    ```bash
    wget https://storage.googleapis.com/chrome-for-testing-public/125.0.6422.60/linux64/chromedriver-linux64.zip
    unzip chromedriver-linux64.zip
    sudo mv chromedriver-linux64/chromedriver /usr/local/bin/
    chromedriver --version
    ```

3. Set up the X Server for browser display:

    ```bash
    export DISPLAY=$(grep nameserver /etc/resolv.conf | awk '{print $2}'):0
    source ~/.bashrc
    ```

## Usage

### Running Individual Scrapers

You can run individual scrapers directly by specifying the desired script. For example:

```bash
python run_scrapers.py -df <date_from> --scrapers <scraper_name>
```

Example:

```bash
python run_scrapers.py -df 2024-05-01 --scrapers MorganStanley
```

### Running All Scrapers

To run all the scrapers at once, use the `run_scrapers.py` script:

```bash
python run_scrapers.py -df <date_from> [--scrapers <scraper1 scraper2 ...>] [--headless] [--overwrite]
```

For example:

```bash
python run_scrapers.py -df 2024-05-01
```

You can also run specific scrapers:

```bash
python run_scrapers.py -df 2024-09-01 --scrapers morgan_stanley goldman
```

### Checking Output in S3

Once the scraping is complete, verify that the reports are stored in your S3 bucket:

```bash
aws s3 ls s3://marketsense-ai/marketdigest_pdfs_db/
```

## Project Structure

- **scrapers/**: Directory containing the individual scrapers.
  - Example scrapers include `blackrock.py`, `goldman.py`, `morgan_stanley.py`, etc.
- **run_scrapers.py**: Manages the parallel execution of multiple scrapers.
- **tmp/**: Temporary storage for downloaded PDF files.
- **poetry.lock** & **pyproject.toml**: Used by Poetry to manage project dependencies.
- **articles_info.json**: Stores metadata or configurations related to the articles.
- **env-assume-role.sh** & **env.sh**: Scripts for environment setup and assuming AWS roles.
- **error.log**: Log file for tracking errors during scraping.

## Troubleshooting

- If you encounter issues with Chrome or ChromeDriver, ensure that the versions match and are installed correctly.
- Check the `error.log` file for any specific errors that occurred during scraping.

If you'd like to contribute to the project, feel free to fork the repository and submit a pull request with your improvements.

## Data Storage Information

The data scraped by the project is stored in an S3 bucket with the following structure:

- **S3 Bucket Name**: `msai`

- **S3 Folder Path**: `v1/shared_data/macro`

  - **`structure/`**: This folder contains JSON files with the processed content extracted from the PDFs or web reports.
    - Each JSON file corresponds to an article or report.
    - The `articles_info.json` file in this folder serves as an index for all articles. It stores metadata for each search and update, allowing tracking and organization of the scraped content.

  - **`pdfs/`**: This folder contains the actual PDF documents that have been scraped. These PDFs are raw, unprocessed reports from various sources (e.g., BlackRock, Morgan Stanley, Goldman Sachs, etc.).

### Flow of Data:

1. **Scrapers**: The scrapers retrieve articles, reports, or documents from various websites.
2. **PDF Storage**: The raw PDF reports are stored in the `v1/shared_data/macro/pdfs/` folder in the S3 bucket.
3. **JSON Metadata & Content**: The extracted content from the PDFs is saved in JSON files and placed in the `v1/shared_data/macro/structure/` folder. The `articles_info.json` file acts as the metadata index, tracking details such as title, date, and other important identifiers.

## S3MacroManager Class Overview

The `S3MacroManager` class in the `macro_handler.py` module is responsible for managing the storage and retrieval of macroeconomic reports and their metadata in an Amazon S3 bucket. Below is a summary of its key functionality:

### Purpose
The `S3MacroManager` class interacts with an S3 bucket to:
- Store PDF reports and their extracted data in JSON format.
- Manage an index file (`articles_info.json`) that tracks all the reports and articles.
- Perform actions like appending new articles, removing articles based on conditions, and retrieving the latest scraped date.

### Key Methods

1. **`_read_file(self, key, download=False, field=None)`**: 
   - Reads a file from S3. Optionally downloads it locally or retrieves specific fields from JSON data.

2. **`store_pdf(self, date, file_name)`**:
   - Uploads a PDF from the local temporary directory to a date-based folder in S3.

3. **`store_json(self, data)`**:
   - Stores JSON content (representing extracted report data) in the `structure/` directory of the S3 bucket.

4. **`get_articles_index(self)`**:
   - Retrieves the `articles_info.json` file, which contains metadata for all articles.

5. **`append_articles_to_index(self, data)`**:
   - Adds new articles to the index, ensuring there are no duplicates.

6. **`store_articles_index(self, data)`**:
   - Saves the updated `articles_info.json` back to the S3 bucket.

7. **`remove_articles(self, date_from, date_to, organization=None)`**:
   - Deletes articles (both the PDF and JSON data) from the S3 bucket based on a date range and optional organization filter.

8. **`get_latest_scrapping_date(self)`**:
   - Returns the latest scraping date for each organization, derived from the `articles_info.json` file.

This class is essential for the storage, retrieval, and management of reports in the S3 environment.

## Scraper Framework Overview

The scraper framework is designed around the `BaseScraper` class, from which all scrapers inherit. This design promotes code reuse, consistency, and ease of extension when adding new scrapers.

### Key Responsibilities of `BaseScraper`:
- **Web Scraping**: Manages browser interactions using Selenium, handles cookies, and allows both headless and non-headless scraping modes.
- **PDF Handling**: Downloads PDFs, renames them, and processes their content.
- **Content Processing**: Extracts text from PDFs and uses language models (e.g., GPT) to clean and summarize content.
- **S3 Integration**: Manages uploading PDFs and JSON metadata to an S3 bucket using `S3MacroManager`.

### Key Methods of `BaseScraper`:

1. **`start_browser()` / `close_browser()`**:
   - Starts or closes the Selenium WebDriver to control the browser. This includes handling cookies for session persistence.

2. **`fetch_articles()`** (abstract):
   - This method must be implemented by any subclass. It defines how to fetch the list of articles from a website.

3. **`extract_article_info(article)`** (abstract):
   - Another method to be implemented by subclasses, responsible for extracting article-specific information (e.g., title, date, URL).

4. **`download_pdf(article_info)`** (abstract):
   - Downloads a PDF associated with an article. This method needs to be implemented by subclasses.

5. **`get_content_and_summary(article_info)`**:
   - Processes the downloaded PDF, extracts its content, and uses an AI model (e.g., GPT) to clean and summarize it.

6. **`process_articles(articles_index_df, date_from, overwrite=False, max_articles=50)`**:
   - Manages the entire process of fetching, downloading, processing, and storing articles. This includes checking if articles are already processed and summarizing their content.

7. **`store_articles(articles)`**:
   - Uploads processed articles (PDFs and JSON) to the S3 bucket using the `S3MacroManager`.

---

### How to Create a New Scraper

1. **Inherit from `BaseScraper`**:
   - Begin by creating a new class for your scraper that inherits from `BaseScraper` and implements the required abstract methods.
   
    ```python
    from base_scraper import BaseScraper
    
    class MyNewScraper(BaseScraper):
        def __init__(self, site_name, base_url, headless=False):
            super().__init__(site_name, base_url, headless)
    
        def fetch_articles(self):
            # Implement logic to scrape articles from the website
            pass
    
        def extract_article_info(self, article):
            # Implement logic to extract article metadata
            pass
    
        def download_pdf(self, article_info):
            # Implement logic to download the article's PDF
            pass
    ```

2. **Implement `fetch_articles`, `extract_article_info`, and `download_pdf`**:
   - Each scraper will have different logic depending on the website, so you'll need to implement the site-specific scraping logic in these methods.

3. **Test the Scraper**:
   - Run your scraper locally to verify it fetches and processes articles correctly. Use the `process_articles()` method from `BaseScraper` to manage the workflow.

4. **Add to `run_scrapers.py`**:
   - Finally, include your new scraper in the `run_scrapers.py` file, so it can be executed alongside other scrapers.

This modular framework ensures that all scrapers follow a consistent pattern, making it easier to maintain and extend the project as more scrapers are added.



##Adding Data to Pinecone

To add data to Pinecone, follow these steps:

1. **Prepare Your Data**: Ensure your that macro scappers have ran. 

2. **Run the Data Ingestion Script**:
    ```bash
    python data_injestion.py --date_from YYYY-MM-DD
    ```
    Replace `YYYY-MM-DD` with the start date to filter reports from.

3. **Environment Variables**:
    Ensure the following environment variables are set:
    ```plaintext
    OPENAI_API_KEY=your_openai_api_key
    PINECONE_API_KEY=your_pinecone_api_key
    ```

4. **Check Pinecone Index**:
    The script will automatically check if the Pinecone index exists and create it if necessary. It will then upsert the data in batches.
