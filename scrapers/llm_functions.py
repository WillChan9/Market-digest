import os
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from pydantic import BaseModel, Field
from langchain_core.output_parsers import JsonOutputParser
from dotenv import load_dotenv, find_dotenv
import tiktoken
from openai import OpenAI
import re
enc = tiktoken.encoding_for_model("gpt-4o-mini")

load_dotenv(find_dotenv())

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


class Article(BaseModel):
    context: str = Field(description="Main analysis, insights, and conclusions and core content of the text in a plain narrative format, not nested in any dictionary.")
    summary: str = Field(description="Concise summary of the cleaned text, limited to 50 words")


def clean_article(text, max_chunk_tokens=125000, overlap_tokens=200):
    if text == '':
        print("Error of article cleaning: No Text")
    else:
        params = {
            'prompt_template': (
                "Extract the core content of the following document, removing all disclaimers, copyrights, and other "
                "non-essential information. Focus on the main analysis, insights, and conclusions. "
                "Provide your response as a JSON with two keys: 'summary' (a concise summary of up to 50 words) and "
                "'context' (the cleaned text presented as a plain narrative without nested dictionaries).\n\n{article}"
            ),
            'model': 'gpt-4o',
            'inputs': ['article']
        }

        # Initialize tokenizer
        tokenizer = enc  # Tiktoken tokenizer initialized earlier
        # Tokenize the text
        tokens = tokenizer.encode(text)
        total_tokens = len(tokens)

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

            # Create the PromptTemplate with the provided input variables
            input_prompt = PromptTemplate(template=params['prompt_template'], input_variables=params['inputs'])

            # Create the ChatOpenAI instance
            llm = ChatOpenAI(temperature=0, model_name=params['model'], max_tokens=4000, openai_api_key=OPENAI_API_KEY)

            # Set up the parser to parse the response into the Article model
            parser = JsonOutputParser(pydantic_object=Article)

            # Create the chain and invoke it with the input text
            chain = input_prompt | llm | parser
            result = chain.invoke(chunk_text)
            analyses.append(result)

        return analyses