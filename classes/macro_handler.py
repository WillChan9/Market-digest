import boto3
import os, uuid, json
import logging

import pandas as pd
from utils.helper_functions import setup_logging

# Set up logging
logger = setup_logging('Macro-Handler', level=logging.INFO)


class S3MacroManager:
    def __init__(self, macro_prefix="macro", bucket_name='msai'):
        self.s3 = boto3.client('s3')
        self.bucket = bucket_name
        self.prefix = macro_prefix

    def _read_file(self, key: str, download=False, field=None):
        try:
            if download:
                random_filename = f"tmp/{str(uuid.uuid4())}.json"
                os.makedirs(os.path.dirname(random_filename), exist_ok=True)
                self.s3.download_file(self.bucket, key, random_filename)
                return random_filename
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            if field:
                return json.loads(content)[field]
            else:
                return json.loads(content)
        except Exception as e:
            logger.warning(f"S3FileManager::_read_file: Unable reading file '{key}':{e}")
            return None

    def store_pdf(self, date, file_name):
        key = f"{self.prefix}/pdfs/{date}/{file_name}"
        try:
            with open('tmp/' + file_name, 'rb') as pdf_data:
                response = self.s3.upload_fileobj(pdf_data, self.bucket, key)
            logger.info(f"File '{file_name}' written successfully in {self.bucket}.")
            return True
        except FileNotFoundError:
            logger.info(f"{file_name} file was not found")
            return False
        except Exception as e:
            logger.info(f"Error uploading {file_name}\n{e}")
            return False

    def read_json(self, file_name):
        file = file_name.replace('pdf', 'json')
        key = f"{self.prefix}/structure/{file}"
        data = self._read_file(key)
        return data

    def store_json(self, data):
        key = f"{self.prefix}/structure/{data['file_name'][:-3]}json"
        content = json.dumps(data)
        try:
            self.s3.put_object(Body=content, Bucket=self.bucket, Key=key)
            logger.info(f"File '{key}' written successfully in {self.bucket}.")
            return True
        except Exception as e:
            logger.error(f"S3FileManager::store_file Error writing file: {e}")
            return False

    def get_articles_index(self):
        key = f"{self.prefix}/structure/articles_info.json"
        data = self._read_file(key)
        # return data
        return json.loads(data)

    def append_articles_to_index(self, data):
        articles_index = self.get_articles_index()
        articles_index.extend(data)
        df = pd.DataFrame(articles_index)

        df_dropped = df.drop(columns=['summary', 'cleaned_text'])
        df_cleaned = df_dropped.dropna(subset=['file_name'])
        df_unique_subset = df_cleaned.drop_duplicates(subset=['Title', 'Date', "file_name"])
        # print( df_unique_subset.sort_values( by=['Date'], ascending = False ) )
        new_articles_info = df_unique_subset.to_json(orient="records")

        self.store_articles_index(new_articles_info)

    def store_articles_index(self, data):
        key = f"{self.prefix}/structure/articles_info.json"
        content = json.dumps(data)
        try:
            self.s3.put_object(Body=content, Bucket=self.bucket, Key=key)
            logger.info(f"File '{key}' written successfully in {self.bucket}.")
            return True
        except Exception as e:
            logger.error(f"S3FileManager::store_file Error writing file: {e}")
            return False

    def remove_articles(self, date_from, date_to, organization=None):
        articles_index = self.get_articles_index()
        df = pd.DataFrame(articles_index)

        # Define the condition based on whether the organization is provided
        if organization:
            condition = (df['Organization'].str.contains(organization, na=False)) & (df['Date'] >= date_from) & (
                        df['Date'] <= date_to)
        else:
            condition = (df['Date'] >= date_from) & (df['Date'] <= date_to)

        # Find matching files based on the condition
        matching_files = df.loc[condition, ['file_name', 'Date']]

        # Delete the matching files from both JSON and PDF directories
        self._delete_files(matching_files)

        # Update the DataFrame by removing the matched rows
        df_cleaned = df[~condition]

        # Store the updated articles index
        new_articles_info = df_cleaned.to_json(orient="records")
        self.store_articles_index(new_articles_info)

    def _delete_files(self, matching_files):
        """Helper function to delete the corresponding JSON and PDF files from S3."""
        for _, row in matching_files.iterrows():
            file_name = row['file_name']
            date = row['Date']

            # Remove the JSON file from the structure directory
            json_key = f"{self.prefix}/structure/{file_name[:-3]}json"
            try:
                self.s3.delete_object(Bucket=self.bucket, Key=json_key)
                logger.info(f"Removed JSON file: {json_key}")
            except Exception as e:
                logger.warning(f"Error removing JSON file {json_key}: {e}")

            # Remove the PDF file from the pdf directory
            pdf_key = f"{self.prefix}/pdfs/{date}/{file_name}"
            try:
                self.s3.delete_object(Bucket=self.bucket, Key=pdf_key)
                logger.info(f"Removed PDF file: {pdf_key}")
            except Exception as e:
                logger.warning(f"Error removing PDF file {pdf_key}: {e}")

    def get_latest_scrapping_date(self):
        """
        Returns a dictionary of the most recent article date for each organization.
        """
        try:
            # Retrieve the articles index data
            articles_index = self.get_articles_index()
            df = pd.DataFrame(articles_index)

            # Clean up the DataFrame to ensure it has proper Date and Organization fields
            df_cleaned = df.dropna(subset=['Organization', 'Date'])

            # Group by Organization and get the most recent date for each
            recent_dates = df_cleaned.groupby('Organization')['Date'].max().reset_index()

            # Convert to a list of dictionaries, or a more usable format
            recent_dates_list = recent_dates.to_dict(orient='records')

            return recent_dates_list

        except Exception as e:
            logger.error(f"Error getting most recent dates for each organization: {e}")
            return None

    def store_marketsense_marketdigest(self, date, data):
        key = f"{self.prefix}/marketsense/marketdigest_{date}.json"
        content = json.dumps(data)
        try:
            self.s3.put_object(Body=content, Bucket=self.bucket, Key=key)
            logger.info(f"File '{key}' written successfully in {self.bucket}.")
            return True
        except Exception as e:
            logger.error(f"S3FileManager::store_file Error writing file: {e}")
            return False

    def store_wix_marketdigest(self, date, data):
        key = f"{self.prefix}/website/marketdigestWix_{date}.json"
        content = json.dumps(data)
        try:
            self.s3.put_object(Body=content, Bucket=self.bucket, Key=key)
            logger.info(f"File '{key}' written successfully in {self.bucket}.")
            return True
        except Exception as e:
            logger.error(f"S3FileManager::store_file Error writing file: {e}")
            return False

    def file_exists_in_s3(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=f"{self.prefix}/{key}")
            return True
        except self.s3.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return False
            else:
                logger.error(f"Error checking existence of file '{key}': {e}")
                return False
