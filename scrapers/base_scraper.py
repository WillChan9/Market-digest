from abc import ABC
import os
import logging
import glob
from .utils import setup_logging
from .macro_handler import S3MacroManager
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from pydantic import BaseModel, Field
from langchain_core.output_parsers import JsonOutputParser
from selenium_stealth import stealth
import tiktoken
import json
from openai import OpenAI

enc = tiktoken.encoding_for_model("gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import pickle
import time, random

logger = setup_logging('BaseScraper', level=logging.ERROR)


class BaseScraper(ABC):
    def __init__(self, site_name, base_url, headless=False, download_dir='tmp'):
        self.site_name = site_name
        self.logger = setup_logging(site_name, level=logging.DEBUG)  # Changed to DEBUG level
        self.logger.debug(f"Initializing BaseScraper for {site_name}")
        self.base_url = base_url
        self.logger.debug(f"Base URL: {base_url}")
        self.headless = headless
        self.logger.debug(f"Headless mode: {headless}")
        self.download_dir = os.path.join(os.getcwd(), download_dir)
        self.logger.debug(f"Download directory: {self.download_dir}")
        self.s3 = S3MacroManager()
        self.logger.debug("S3MacroManager initialized")
        os.makedirs(self.download_dir, exist_ok=True)
        self.logger.debug(f"Created download directory: {self.download_dir}")
        self.driver = None
        self.cookies_file = f'{site_name}_cookies.pkl'
        self.logger.debug(f"Cookies file: {self.cookies_file}")
        # Remove cookies if they exist
        self.remove_cookies()
        self.logger.debug("Cookies removed (if existed)")

    def remove_cookies(self):
        if os.path.exists(self.cookies_file):
            os.remove(self.cookies_file)

    def get_driver_options(self):
        options = Options()
        if self.headless:
            options.add_argument('--headless=new')  # Updated headless argument
            options.add_argument('--disable-blink-features=AutomationControlled')  # Helps avoid detection
            self.logger.info('Running headless mode')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')  # Disable GPU acceleration
        options.add_argument('--window-size=1920,1080')  # Optional: Set a custom window size
        options.add_argument('--disable-extensions')  # Disable extensions
        options.add_argument('--disable-notifications')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--start-maximized')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36')  # Add realistic user agent
        options.add_argument('--referrer=https://www.google.com')  # Add referrer to appear as if coming from Google


        options.add_experimental_option('prefs', {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
            "javascript.enabled": True,
            "intl.accept_languages": "en-US,en"  # Set accepted languages
        })
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        return options

    def start_browser(self):
        """Start the Selenium WebDriver if not already started."""
        self.logger.debug("Starting browser")
        if not self.driver:
            self.logger.debug("WebDriver not initialized, creating new instance")
            self.driver = webdriver.Chrome(options=self.get_driver_options())
            
            # Add stealth configuration
            stealth(self.driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            
            self.logger.debug("WebDriver instance created with stealth configuration")
               
            # Load cookies before navigating to the base URL
            self.logger.debug("Attempting to load cookies")
            cookies_loaded = self.load_cookies()

            if not cookies_loaded:
                self.logger.debug("No cookies loaded, navigating directly to base URL")
                # No cookies were loaded, navigate to the base URL directly
                self.driver.get(self.base_url)
                self.logger.debug(f"Navigated to {self.base_url}")
            else:
                self.logger.debug("Cookies loaded, reloading page to apply them")
                # Cookies were loaded, reload the page to apply them
                self.driver.get(self.base_url)  # Load the base URL first
                self.logger.debug(f"Loaded base URL: {self.base_url}")
                self.driver.refresh()  # Refresh to ensure cookies are applied
                self.logger.debug("Page refreshed to apply cookies")
        else:
            self.logger.debug("WebDriver already initialized")

    def close_browser(self):
        """Close the Selenium WebDriver if open."""
        if self.driver:
            self.save_cookies()  # Save cookies before quitting
            self.driver.quit()
            self.driver = None
            self.remove_cookies()

    def save_cookies(self):
        """Save cookies to a file."""
        if self.driver:
            cookies = self.driver.get_cookies()
            with open(self.cookies_file, 'wb') as f:
                pickle.dump(cookies, f)

    def load_cookies(self):
        """Load cookies from a file if they exist. Returns True if cookies were loaded, False otherwise."""
        if os.path.exists(self.cookies_file):
            with open(self.cookies_file, 'rb') as f:
                cookies = pickle.load(f)
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
            self.logger.info(f"Loaded cookies from {self.cookies_file}.")
            return True
        return False
    
    def fetch_articles(self):
        """Fetch articles from the website. To be implemented by child classes."""
        pass

    def extract_article_info(self, article):
        """Extract and return necessary information from a single article."""
        pass

    def download_pdf(self, article_info):
        pass

    def rename_downloaded_file(self, new_filename):
        list_of_files = glob.glob(os.path.join(self.download_dir, '*'))
        if not list_of_files:
            self.logger.error(f"No files found in {self.download_dir} to rename.")
            return None
        latest_file = max(list_of_files, key=os.path.getctime)
        try:
            new_filepath = os.path.join(self.download_dir, new_filename)
            os.rename(latest_file, new_filepath)
            return new_filename
        except Exception as e:
            self.logger.error(f"Failed to rename {latest_file} to {new_filename}: {e}")
            return None

    def isMacro(self, text):
            # Initialize tokenizer
            tokenizer = enc  # Tiktoken tokenizer initialized earlier
            # Tokenize the text
            tokens = tokenizer.encode(text)
            total_tokens = len(tokens)

            # keep the first part of the text that fits in the gpt model
            end = min(125000, total_tokens)
            chunk = tokens[:end]
            chunk_text = tokenizer.decode(chunk)
            params = {'filter_macro': {
            'prompt': (
                "As a financial expert, analyze the given text to determine if it includes actionable macroeconomic insights "
                "or tradable ideas relevant to the stock markets. Consider factors such as market trends, economic "
                "indicators, investment opportunities, and risk assessments. "
                "Respond with either 'yes' or 'no'.\n\n{article}\n\nRespond in 1 word with 'yes' or 'no':"
            ),
            'model': 'gpt-4o-mini',
            'inputs': ['article']},
            }

            # Create the PromptTemplate with the provided input variables
            input_prompt = PromptTemplate(template=params['filter_macro']['prompt'], input_variables=params['filter_macro']['inputs'])

            # Create the ChatOpenAI instance
            llm = ChatOpenAI(temperature=0, model_name=params['filter_macro']['model'], max_tokens=5, openai_api_key=os.getenv("OPENAI_API_KEY"))

            # Chain for the OpenAI call
            chain = input_prompt | llm
            ismacro = chain.invoke({'article': chunk_text}).content.lower()
            if ismacro == 'yes':
                self.logger.info(f"Article is macro: {ismacro}")
                return True
            else:
                self.logger.info(f"Article is not macro: {ismacro}")
                return False
        
    def get_content_and_summary(self, article_info):
        """Process the downloaded PDF, extract content, and summarize it."""
        file_name = article_info['file_name']
        pdf_path = os.path.join(self.download_dir, file_name)

        if not os.path.exists(pdf_path):
            self.logger.error(f"File {pdf_path} does not exist.")
            return None

        try:
            loader = PyPDFLoader(pdf_path)
            pages = loader.load_and_split()

            if not pages:
                self.logger.error(f"Failed to read pages from PDF: {file_name}")
                return None

            content = ' '.join([page.page_content for page in pages])
            
            if self.isMacro( content ):
                clean_content = self.clean_article(content)
                if clean_content:
                    return clean_content
                else:
                    self.logger.error(f"Error cleaning article: {file_name}")
                    return None
            else:
                self.logger.warning(f"{file_name} is not consider Macro document, pass")                 

        except Exception as e:
            self.logger.error(f"Error reading PDF file {file_name}: {e}")
            return None

    def clean_article(self, text, max_chunk_tokens=30000, overlap_tokens=200):

        if not text:
            self.logger.error("Error in article cleaning: No Text provided.")
            return None

        try:
            # Initialize tokenizer
            tokenizer = enc  # Tiktoken tokenizer initialized earlier
            tokens = tokenizer.encode(text)
            total_tokens = len(tokens)

            if total_tokens > max_chunk_tokens:
                # Split tokens into chunks with overlap
                chunks = []
                start = 0
                while start < total_tokens:
                    end = min(start + max_chunk_tokens, total_tokens)
                    chunk_tokens = tokens[start:end]
                    chunks.append(chunk_tokens)
                    start += max_chunk_tokens - overlap_tokens  # Move start forward

                # Process each chunk
                analyses = []
                for idx, chunk_tokens in enumerate(chunks):
                    chunk_text = tokenizer.decode(chunk_tokens)

                    messages=[{
                            "role": "system",
                            "content": [
                                { "type": "text",
                                  "text": "You are a financial analyst. Extract the core content from a provided financial article, report, or expert analysis, omitting all disclaimers, copyrights, and other non-essential information. Summarize the main analysis, insights, and key conclusions, while retaining only the most informative and relevant parts.\n\n# Steps\n\n1. **Initial Reading**: Read the entire article and recognize different components within (e.g., analysis, advertisements, disclaimers, copyrights, etc.)\n2. **Identification of Content**:\n   - Identify and separate analysis and insights from non-essential information.\n   - Note and discard disclaimers, copyright notices, advertisements, or anything unrelated to financial interpretation.\n3. **Summarize Core Content**:\n   - Extract the main points, retaining the core analysis, key insights, and conclusions.\n   - Ensure the focus remains on financial insights, rationale, and associated data without extra commentary.\n4. **Conclusion Check**: Verify that core takeaways are represented clearly in a concise manner.\n\n# Output Format\n\nProvide the output as a **summary text** containing only the main analysis, key insights, and conclusions. This text should be up to **5000 words**, depending on the length and complexity of the original content.\n\n"
                                }]
                        },
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": chunk_text}]
                        }]

                    # Make the API call for the chunk
                    response = client.chat.completions.create(model='gpt-4o-mini',messages=messages,temperature=0, max_tokens=4000)
                    # Extract the response content
                    result = response.choices[0].message.content
                    analyses.append(result)

                # Combine the analyses into a text to feed into the overall analysis
                chunk_analyses_text = "\n\n".join([f"Analysis of chunk {idx+1}:\n{analysis}" for idx, analysis in enumerate(analyses)])

                # Now, create a new prompt to produce the overall analysis
                messages=[
                    {"role": "system", "content": [{"type": "text","text": "You are a helpful assistant. You are given analyses of several chunks of a document. Your task is to create an overall analysis and summary of the document based on these chunk analyses. Focus on synthesizing the main points, insights, and conclusions. Provide your response in JSON format with the following structure:\n\n{\n  'summary': 'A concise summary of the document, up to 50 words.',\n  'cleaned_text': 'The overall cleaned text, presented as a plain narrative without nested dictionaries.'\n}"}]},
                    {"role": "user", "content": [{"type": "text","text": chunk_analyses_text}]}
                    ]
                
                response = client.chat.completions.create(model='gpt-4o-mini',messages=messages,temperature=0, max_tokens=8000, response_format={"type": "json_object"})
                result = json.loads(response.choices[0].message.content)
                print(f"\n\ntype(result): {type(result)}\n\n")
                print(result)
                return result
            else:
                # Directly process the text if within token limit
                messages=[
                    {"role": "system", "content": [{"type": "text", "text": "You are a financial analyst tasked with creating an investor-focused report based on a provided financial article, report, or expert analysis. Exclude all non-essential information (e.g., disclaimers, copyright notices, advertisements) and deliver a structured, in-depth analysis that highlights core ideas, findings, and key insights relevant to investment decision-making.\n\n# Steps\n\n1. **Initial Review**: Thoroughly read the entire article, distinguishing critical financial insights, key metrics, and actionable findings from any extraneous content.\n\n2. **Investor-Focused Content Structuring**:\n   - Extract essential financial insights, metrics, data, and interpretations.\n   - Prioritize sections with high investor relevance, such as market trends, risk factors, growth opportunities, and economic impacts.\n   - Organize the content into a coherent narrative, presenting an investor-oriented analysis with practical implications.\n\n3. **Report Detailing for Investor Context**:\n   - Develop a comprehensive report highlighting key findings, relevant metrics, and any financial indicators with potential impacts on investment strategies.\n   - Clearly communicate the rationale behind insights, implications for market behavior, potential risks, and opportunities, ensuring relevance to investors.\n\n4. **Verification**:\n   - Confirm that the `cleaned_text` captures all significant insights, financial implications, and investor-relevant conclusions.\n   - Ensure the report is structured to offer a complete, coherent narrative focused on investment takeaways.\n\n# Output Format\n\nProvide your response in JSON format with the following structure:\n\n{\n  \"summary\": \"A high-level summary of the main investor takeaways, limited to 50 words.\",\n  \"cleaned_text\": \"A detailed, investor-focused report that thoroughly presents core ideas, findings, financial insights, and investment implications of the original document in a structured, narrative style,  limited to 5000 words.\"\n}\n"}]},
                    {"role": "user", "content": [{"type": "text","text": text}]}
                    ]
                
                response = client.chat.completions.create(model='gpt-4o-mini',messages=messages,temperature=0, max_tokens=6000, response_format={"type": "json_object"})
                result = json.loads(response.choices[0].message.content)
                return result

        except Exception as e:
            self.logger.error(f"Error cleaning article: {e}")
            return None
   
    def parse_llm_response(self, response):
        """Parse the JSON response from the LLM."""
        try:
            parsed_data = response  # Assuming the LLM's output is already parsed JSON
            return {
                'summary': parsed_data['summary'],
                'cleaned_text': parsed_data['cleaned_text']
            }
        except (KeyError, ValueError) as e:
            self.logger.error(f"Error parsing LLM response: {e}")
            return None

    def process_articles(self, articles_index_df, date_from, overwrite=False, max_articles = 50):
        """Process articles and download PDFs, summarize them, and update records."""
        
        self.logger.info("Starting process_articles function.")
        
        # Start the browser session
        self.start_browser()  
        self.logger.info("Browser started and base URL loaded.")
        
        try:
            # Fetch the articles
            articles = self.fetch_articles()

            self.logger.info(f"Fetched {len(list(articles))} articles from the website.")

        except Exception as e:
            self.logger.error(f"Error fetching articles: {e}")
            self.close_browser()
            return []

        new_articles = []

        for idx, article in enumerate(articles):
            if idx >= max_articles:
                self.logger.info(f'Reached maximum number of articles {max_articles}')
                break
            try:
                # Extract article info
                article_info = self.extract_article_info(article)
                if not article_info:
                    self.logger.warn(f'Article {idx} has not been processed')
                    continue        

                self.logger.debug(f"Article info extracted: {article_info}")

                # Check article date
                if article_info['Date'] < date_from:
                    # self.logger.info(f"Article '{article_info['Title']}' {article_info['Date']} is older than the date_from {date_from}, skipping.")
                    continue

                # Check for existing records
                existing_records = articles_index_df[
                    (articles_index_df['Title'].str.lower() == article_info['Title'].lower()) &
                    (articles_index_df['file_name'].str.lower() == article_info['file_name'].lower())
                ]

                if not existing_records.empty and not overwrite:
                    self.logger.info(f"Article '{article_info['Title']}' - {article_info['Date']} already exists, skipping.")
                    continue

                # Download the PDF
                downloaded = self.download_pdf(article_info)
                if not downloaded:
                    self.logger.error(f"Failed to download PDF for article '{article_info['Title']}'. Skipping article.")
                    continue

                # Process and summarize the content
                clean_content = self.get_content_and_summary( article_info )
                if clean_content:
                    self.logger.info(f"Content processed for article '{article_info['Title']}' - {article_info['Date']}")
                    article_info.update(clean_content)
                    new_articles.append(article_info)

            except Exception as e:
                self.logger.error(f"Error processing article '{article_info['Title']}': {e}")
                continue

        # Close the browser session
        self.close_browser()
        self.logger.info("Browser closed after processing articles.")

        if new_articles:
            self.logger.info(f"{len(new_articles)} new articles processed.")
        else:
            self.logger.info("No new articles were processed.")

        return new_articles

    def store_articles(self, articles):
        for article in articles:
            self.s3.store_pdf(article['Date'], article['file_name'])
            self.s3.store_json(article)

        if articles:
            self.s3.append_articles_to_index(articles)
        else:
            self.logger.info("No new articles to append.")
