import os

import pandas as pd

from minio import Minio

import logging

from dotenv import load_dotenv
 
# Load environment variables from the .env file

load_dotenv(dotenv_path='api.env')
 
# Configure logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
# Retrieve MinIO credentials from environment variables

minio_host = os.getenv('MINIO_HOST')

minio_access_key = os.getenv('MINIO_ACCESS_KEY')

minio_secret_key = os.getenv('MINIO_SECRET_KEY')

minio_secure = os.getenv('MINIO_SECURE', 'False').lower() == 'true'  # Ensure it's correctly interpreted as a boolean
 
if not minio_access_key or not minio_secret_key:

    logging.error("MinIO credentials are not set in the environment variables.")

    raise EnvironmentError("Please set 'MINIO_ACCESS_KEY' and 'MINIO_SECRET_KEY' in your environment.")
 
# Initialize the MinIO client with secure=False for HTTP

client = Minio(

    minio_host,

    access_key=minio_access_key,

    secret_key=minio_secret_key,

    secure=minio_secure

)
 
# Function to print bucket names

def print_bucket_names():

    try:

        buckets = client.list_buckets()

        if buckets:

            logging.info("Buckets in MinIO:")

            for bucket in buckets:

                print(bucket.name)

        else:

            logging.info("No buckets found in MinIO.")

    except Exception as e:

        logging.error(f"Error listing buckets: {e}")
 
# Call the function to print the bucket names

print_bucket_names()
 
# CSV Preprocessing Functions

def handle_missing_values(df):

    """Handle missing values in the DataFrame."""

    for column in df.select_dtypes(include=['float64', 'int64']).columns:

        if df[column].isnull().any():  # Check if there are any missing values

            if not df[column].dropna().empty:  # Check if the column is not completely empty

                df[column] = df[column].fillna(df[column].median())  # Fill with median

            else:

                logging.warning(f"Column '{column}' is completely empty. Skipping median fill.")

    return df
 
def preprocess_csv(file_path):

    """Load and preprocess CSV data."""

    try:

        # Try loading the CSV file normally

        df = pd.read_csv(file_path)

        logging.info(f"Columns in {file_path} (initial load): {df.columns.tolist()}")
 
        # If there are any missing columns or if the DataFrame is empty, try again with no header

        if df.empty or df.columns.isnull().any():

            logging.info("Attempting to use the first row as column headers.")

            df = pd.read_csv(file_path, header=None)  # Read without headers

            df.columns = df.iloc[0]  # Set the first row as header

            df = df[1:]  # Remove the first row from the data

            df.reset_index(drop=True, inplace=True)
 
            logging.info(f"Columns in {file_path} (after setting header): {df.columns.tolist()}")
 
        # Handle missing values

        df = handle_missing_values(df)
 
        # Remove completely empty columns

        df.dropna(axis=1, how='all', inplace=True)

        logging.info(f"Columns after dropping empty columns: {df.columns.tolist()}")
 
        logging.info("Data preprocessing complete.")

        return df
 
    except Exception as e:

        logging.error(f"Error processing {file_path}: {e}")

        return None
 
def copy_objects(source_bucket, dest_bucket, file_path):

    """Copy files from source bucket to destination bucket."""

    try:

        client.fput_object(dest_bucket, os.path.basename(file_path), file_path)

        logging.info(f"Copied {file_path} to {dest_bucket}.")

    except Exception as err:

        logging.error(f"Error copying {file_path} to {dest_bucket}: {err}")
 
def preprocess_all_csv_files_in_bucket(bucket_name, silver_bucket_name):

    """Preprocess all CSV files in the specified MinIO bucket and move to silver bucket."""

    processed_files = []  # To keep track of processed files

    skipped_files = []    # To keep track of skipped files

    try:

        objects = client.list_objects(bucket_name, recursive=True)

        for obj in objects:

            if obj.object_name.endswith('/') or not obj.object_name.endswith('.csv'):

                continue  # Skip directories and non-CSV files

            temp_file_path = f'/tmp/{obj.object_name}'

            os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)

            client.fget_object(bucket_name, obj.object_name, temp_file_path)

            try:

                processed_data = preprocess_csv(temp_file_path)

                if processed_data is not None:

                    processed_files.append(obj.object_name)

                    logging.info(f"Processed data for {obj.object_name}.")

                    # Save the processed data locally first

                    processed_data.to_csv(temp_file_path, index=False)  # Save locally
 
                    # Copy the processed file to the silver bucket

                    copy_objects(bucket_name, silver_bucket_name, temp_file_path)

                    # Clean up temporary file

                    os.remove(temp_file_path)

                else:

                    skipped_files.append(obj.object_name)

                    logging.warning(f"Skipping processing for {obj.object_name} due to validation issues.")

            except Exception as e:

                logging.error(f"Error processing {obj.object_name}: {e}")

                os.remove(temp_file_path)
 
        # Summary of processing

        logging.info("Preprocessing complete.")

        logging.info(f"Processed files: {processed_files}")

        logging.info(f"Skipped files: {skipped_files}")
 
        # Optionally list all files in the silver bucket

        silver_objects = client.list_objects(silver_bucket_name, recursive=True)

        silver_files = [obj.object_name for obj in silver_objects]

        logging.info(f"Files in silver bucket after processing: {silver_files}")

    except Exception as err:

        logging.error(f"Error listing objects in bucket {bucket_name}: {err}")
 
# Example usage

preprocess_all_csv_files_in_bucket('dw-bucket-bronze', 'dw-bucket-silver')

 