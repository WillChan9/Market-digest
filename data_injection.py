import logging, os, argparse
from classes.macro_handler import S3MacroManager
import pandas as pd
from utils.helper_functions import setup_logging

from typing import List
import time
from pinecone import ServerlessSpec, Pinecone
from tqdm import tqdm
from langchain_experimental.text_splitter import SemanticChunker
from langchain_openai.embeddings import OpenAIEmbeddings

import datetime

logger = setup_logging('Data Injestion', level=logging.INFO)

EMBED_MODEL = "text-embedding-3-small"
INDEX_NAME = 'macro'
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY')
pc = Pinecone(api_key=PINECONE_API_KEY)
embedding_model = OpenAIEmbeddings(model=EMBED_MODEL)



def get_embeddings(text_list) -> List[List[float]]:
    try:
        embeddings = embedding_model.embed_documents(text_list)
        return embeddings
    except Exception as e:
        logger.error(f"Error generating embeddings: {e}")
        return []


def chunk_text(text, max_tokens=500, overlap=50):
    """
    Splits the text into chunks of max_tokens size with overlap.
    """
    import tiktoken
    tokenizer = tiktoken.encoding_for_model('gpt-4o-mini')

    tokens = tokenizer.encode(text)
    chunks = []
    start = 0
    end = max_tokens

    while start < len(tokens):
        chunk_tokens = tokens[start:end]
        chunk_text = tokenizer.decode(chunk_tokens)
        chunks.append(chunk_text)
        start += max_tokens - overlap
        end = start + max_tokens

    return chunks



def get_reports(date_from):
    s3 = S3MacroManager()
    articles = pd.DataFrame(s3.get_articles_index())
    articles_filtered = articles[articles.Date > date_from].reset_index(drop=True)
    logger.info(f"Got {len(articles_filtered)} documents")
    files = articles_filtered.file_name.to_list()
    
    data = []
    for f in files:
        file = f.replace('pdf', 'json')
        try:
            data.append(s3.read_json(file))
        except AttributeError:
            logger.error(f'Error finding {file}')
            continue

    df = pd.DataFrame(data)
    if 'cleaned_text' in df.columns:
        df['cleaned_text'] = df['cleaned_text'].apply(lambda x: ". ".join([f"{k}: {v}" for k, v in x.items()]) if isinstance(x, dict) else x)
    df['cleaned_text'] = df['cleaned_text'].apply(lambda x: x.replace('\n', ' '))
    df['Timestamp'] = pd.to_datetime(df['Date']).astype('datetime64[s]')

    # **Add this line to create the 'id' column**
    df['id'] = df['Date'].str.replace('-', '') + '_' + df['Title'].str.replace(' ', '').str.encode('ascii', 'ignore').str.decode('ascii')

    # Implement chunking
    chunked_data = []
    
    # Initialize the SemanticChunker with the Gradient method
    text_splitter = SemanticChunker(
        embedding_model,
        breakpoint_threshold_type="gradient"
    )

    for idx, row in df.iterrows():
        text = row['cleaned_text']
        if not text:
            continue
        # Create documents using SemanticChunker
        docs = text_splitter.create_documents([text])

        for i, doc in enumerate(docs):
            chunk_id = f"{row['id']}_chunk_{i}"
            chunked_data.append({
                'id': chunk_id,
                'chunk_text': doc.page_content,
                'Organization': row['Organization'],
                'Date': row['Date'],
                'Timestamp': row['Timestamp'],
                'Title': row['Title'],
                'Link': row['Link'],
                'summary': row.get('summary', ''),
                'source_id': row['id']  # Reference to the original document
            })

    chunked_df = pd.DataFrame(chunked_data)
    embeddings = get_embeddings(chunked_df['chunk_text'].to_list())
    chunked_df['embeddings'] = embeddings
    if chunked_df.empty:
        logger.warning("No data to process after chunking.")
    return chunked_df



def check_and_create_index(index_name, dimension, spec):
    if index_name not in pc.list_indexes().names():
        pc.create_index(
            index_name,
            dimension=dimension,
            metric='dotproduct',
            spec=spec
        )
        while not pc.describe_index(index_name).status['ready']:
            time.sleep(1)

def save_to_pinecone(df):
    spec = ServerlessSpec(cloud="aws", region="us-east-1")
    check_and_create_index(INDEX_NAME, len(df['embeddings'][0]), spec)

    index = pc.Index(INDEX_NAME)
    time.sleep(1)
    logger.info(index.describe_index_stats())

    batch_size = 32  # process everything in batches of 32
    for i in tqdm(range(0, len(df), batch_size)):
        # set end position of batch
        i_end = min(i+batch_size, len(df))
        # get batch of IDs
        ids_batch = df['id'][i: i_end]
        # prep metadata and upsert batch
        metadata = [
            {
                "Organization": df.loc[j, "Organization"],
                "Date": df.loc[j, "Date"],
                "Timestamp": df.loc[j, "Timestamp"].timestamp(),
                "Title": df.loc[j, "Title"],
                "Link": df.loc[j, "Link"],
                "summary": df.loc[j, "summary"],
                "text": df.loc[j, "chunk_text"],  # Use chunk_text instead of cleaned_text
                "source_id": df.loc[j, "source_id"]
            }
            for j in range(i, i_end)
        ]
        to_upsert = zip(ids_batch, df['embeddings'][i:i_end], metadata)
        # upsert to Pinecone
        r = index.upsert(vectors=list(to_upsert))
        logger.info(r)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Injestion Script")
    parser.add_argument('-d', '--date_from', type=str, required=False, help='The start date to filter reports from (YYYY-MM-DD). Defaults to 3 days ago if not provided.')
    parser.add_argument('--refresh-container', action='store_true', help='Refresh the ECS container after processing')
    args = parser.parse_args()

    if args.date_from:
        date_from = args.date_from
    else:
        # Default to 2 days ago if date_from is not provided
        date_from = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

    logger.info(f"Processing reports from {date_from}")

    df = get_reports(date_from=date_from)
    if not df.empty:
        save_to_pinecone(df)
    else:
        logger.warning("No data to save to Pinecone.")

    if args.refresh_container:
        logger.info("Refreshing ECS container...")
        import boto3
        ecs = boto3.client('ecs', region_name='eu-central-1')
        try:
            response = ecs.update_service(
                cluster='marketsense-cluster',
                service='macro-chat-service',
                forceNewDeployment=True
            )
            logger.info("Container refreshed successfully.")
        except Exception as e:
            logger.error(f"Failed to refresh ECS container: {str(e)}")