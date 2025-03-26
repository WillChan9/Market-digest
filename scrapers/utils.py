import logging
import os, json
from datetime import datetime
from unidecode import unidecode
from langchain_community.document_loaders import PyPDFLoader
from langchain_openai import ChatOpenAI
import tiktoken
enc = tiktoken.encoding_for_model("gpt-4o-mini")

import logging
from .llm_functions import clean_article

def setup_logging(logger_name, level=logging.INFO, log_file='error.log'):
    logger = logging.getLogger(logger_name)
    if not logger.handlers:  # Check if the logger already has handlers
        # Create a console handler and set the level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(console_handler)

        # Create a file handler for logging errors to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        logger.addHandler(file_handler)

    logger.setLevel(level)
    return logger

logger = setup_logging('Utils', level=logging.INFO)


def days_between(date_str):
    given_date = datetime.strptime(date_str, '%Y-%m-%d').date()  # Convert to a date object
    current_date = datetime.now().date()
    delta = current_date - given_date
    return max(0, delta.days)


def sanitize_filename(filename):
    """Clean up the invalid symbols in file name."""
    illegal_characters = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for char in illegal_characters:
        filename = filename.replace(char, '')
    return filename

def clean_text(text):
    return unidecode(text)

def rename_latest_file(download_dir, filename):
    """Rename the most recently modified file in the download directory."""
    try:
        files = [os.path.join(download_dir, f) for f in os.listdir(download_dir)]
        latest_file = max(files, key=os.path.getctime)
        new_file_path = os.path.join(download_dir, filename)
        os.rename(latest_file, new_file_path)
        return new_file_path
    except ValueError:
        logger.error("No files found in the directory.")
        return None


def parse_text_from_pdf(file_path):
    # file_path = os.path.join(folder_name, file_name)

    # Check if the file exists
    if not os.path.exists(file_path):
        logger.error(f"File {file_path} does not exist.")
        return ""

    try:
        loader = PyPDFLoader(file_path)
        pages = loader.load_and_split()

        # Ensure pages are read correctly
        if not pages:
            logger.error(f"Failed to read pages from PDF: {file_path}")
            return ""

        content = ''
        for page in pages:
            content += ' ' + page.page_content

        # Check if content is read properly
        if not content.strip():
            logger.error(f"No content read from PDF: {file_path}")
            return ""

        return content.strip()

    except Exception as e:
        logger.error(f"Error reading PDF file {file_path}: {e}")
        return ""

                

def isMacro(text, max_chunk_tokens=125000):
    from langchain.prompts import PromptTemplate

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

    # Initialize tokenizer
    tokenizer = enc  # Tiktoken tokenizer initialized earlier

    # Tokenize the text
    tokens = tokenizer.encode(text)
    total_tokens = len(tokens)

    # keep the first part of the text that fits in the gpt model
    end = min(max_chunk_tokens, total_tokens)
    chunk = tokens[:end]


    # Create the PromptTemplate with the provided input variables
    input_prompt = PromptTemplate(template=params['filter_macro']['prompt'],
                                input_variables=params['filter_macro']['inputs'])

    # Create the ChatOpenAI instance
    llm = ChatOpenAI(temperature=0, model_name=params['filter_macro']['model'], max_tokens=5,
                    openai_api_key=os.getenv("OPENAI_API_KEY"))

    # Chain for the OpenAI call
    chain = input_prompt | llm
    ismacro = chain.invoke(chunk).content.lower()
    if ismacro == 'yes':
        return True
    else:
        return False
    
def get_content_and_summary(file_name):
    DOWNLOAD_DIR = os.path.join(os.getcwd(), 'tmp')
    context = parse_text_from_pdf(DOWNLOAD_DIR +'/'+ file_name)
    if isMacro(context):
        return clean_article(context)
    else:
        logger.warning(f"{file_name} is not consider Macro document, pass")                     




def extract_article_info_from_pdf(pdf_text):
    from openai import OpenAI
    # Initialize the OpenAI client
    client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

    # Prepare the data
    provided_data = {
        'pdf_text': pdf_text
    }

    # Remove None values for handling older versions
    provided_data = {k: v for k, v in provided_data.items() if v is not None}

    # Prepare the messages
    system_message = 'You are an expert financial analyst'
    human_message = """
    Your task is to identify the published date (YYYY-MM-DD), title (no special characters) and description (few words) from the provided text:

    {pdf_text}

    Your response in json format with the folowing keys: Date, Title, Description

    """.format(**provided_data)

    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": system_message}]
        },
        {
            "role": "user",
            "content": [{"type": "text", "text": human_message}]
        }
    ]

    # Make the API call
    response = client.chat.completions.create(
        model='gpt-4o-mini',
        messages=messages,
        temperature=0,
        seed = 1042,
        frequency_penalty=0,
        presence_penalty=0,
        response_format={"type": "json_object"}
    )

    # Extract the response content
    result = response.choices[0].message.content
    # Parse the JSON response
    result = json.loads(result)
    return result