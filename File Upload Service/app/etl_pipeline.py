from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, mean, stddev, lit, monotonically_increasing_id
from minio import Minio
from minio.error import S3Error
import os
import io  # Import for handling byte streams
from datetime import datetime
import sys
from pyspark.sql.types import NumericType
from pyspark.sql.utils import AnalysisException
import logging
import re


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# MinIO creds
minio_client = Minio(
    "10.137.0.149:9000",  # Minio IP
    access_key=os.getenv('AWS_ACCESS_KEY_ID'),  
    secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),  
    secure=False  
)

# start up spark session with Minio using parquet (instead of Deltatables and no longer iceberg)

spark = SparkSession.builder \
    .appName("ETL with Spark and Parquet") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.137.0.149:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

# for ETL the source will be coming from bronze with original data and the result will be stored in silver.
source_bucket = "dw-bucket-bronze" 
destination_bucket = "dw-bucket-silver"
metadata_bucket = "dw-bucket-metadata"  # Bucket to store metadata of processed files

def list_files_in_bucket(bucket_name):
    """List all files in a specified MinIO bucket."""
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        file_list = [obj.object_name for obj in objects]
        return file_list
    except S3Error as e:
        print(f"Error listing files in bucket {bucket_name}: {e}")
        return []

def is_file_in_bucket(bucket_name, file_name): # add in the check for the file in bronze before silver prepro
    """Check if a specific file exists in the specified bucket."""
    try:
        minio_client.stat_object(bucket_name, file_name)
        return True
    except S3Error as e:
        if e.code == 'NoSuchKey':
            return False
        else:
            print(f"Error checking file in bucket {bucket_name}: {e}")
            return False


def is_file_processed(file_name):
    """Check if a file has already been processed by looking for it in the metadata bucket."""
    try:
        processed_files = list_files_in_bucket(metadata_bucket)
        return file_name in processed_files
    except S3Error as e:
        print(f"Error checking processed files in metadata bucket: {e}")
        return False

def mark_file_as_processed(file_name):
    """Mark a file as processed by uploading an empty object with the file name to the metadata bucket."""
    try:
        # Upload an empty object with the file name to mark it as processed
        minio_client.put_object(metadata_bucket, file_name, io.BytesIO(b''), 0)
        print(f"Marked file {file_name} as processed.")
    except S3Error as e:
        print(f"Failed to mark file {file_name} as processed: {e}")

# preprocessing option 1 - basic cleanup
def apply_basic_cleanup(df):
    """Basic data clean up: remove rows where all but one column is missing data,
    remove duplicates, remove entirely blank columns, standardize column names,
    add extract date, and unique ID."""
    logger.info("Applying basic data clean up...")

    # Initialize a list to hold columns that don't cause errors
    valid_columns = []

    # Step 1: Remove columns that are entirely blank, null, or empty
    for col_name in df.columns:
        try:
            # Check if the column has at least one non-null and non-empty value
            non_null_count = df.filter(col(col_name).isNotNull() & (col(col_name) != "")).limit(1).count()
            if non_null_count > 0:
                valid_columns.append(col_name)
            else:
                logger.info(f"Dropping column '{col_name}' as it is entirely blank or null.")
        except Exception as e:
            logger.error(f"Error processing column '{col_name}': {e}")
            logger.info(f"Skipping column '{col_name}' due to error.")
            continue

    # Select only the valid columns
    df = df.select(valid_columns)

    # Step 2: Standard column names for governance
    new_column_names = []
    columns_to_drop = []

    for col_name in df.columns:
        try:
            # Standardize column name: lowercase, replace special characters with underscores
            new_col_name = re.sub(r'[^0-9a-zA-Z]+', '_', col_name.strip().lower()).strip('_')
            new_column_names.append(new_col_name)
        except Exception as e:
            logger.error(f"Error renaming column '{col_name}': {e}")
            logger.info(f"Dropping column '{col_name}' due to error.")
            columns_to_drop.append(col_name)

    # Drop columns that caused errors during renaming
    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        # Remove corresponding names from new_column_names
        new_column_names = [name for idx, name in enumerate(new_column_names) if df.columns[idx] not in columns_to_drop]

    # Apply new column names
    df = df.toDF(*new_column_names)

    # Step 3: Remove rows where all but one column is missing data
    min_non_null_values = 2  # At least two non-null values required to keep the row
    df = df.dropna(thresh=min_non_null_values)

    # Step 4: Remove duplicate rows
    df = df.dropDuplicates()

    # Step 5: Add extract date column
    extract_date = datetime.now().strftime('%Y-%m-%d')
    df = df.withColumn("extract_date", lit(extract_date))

    # Step 6: Add unique ID column
    df = df.withColumn("unique_id", monotonically_increasing_id())

    return df

# Preprocessing option 2
def apply_ml_preprocessing(df):
    """Preprocessing for Machine Learning: fill missing values, scale numeric features
        The ML preprocessing aims to pre-perform some of the fundamental changes required to perform ML
        this function detects datatypes that are able to """
    logger.info("Applying preprocessing for Machine Learning...")
    for column in df.columns:
        try:
            # Get the data type of the column
            dtype = df.schema[column].dataType

            # Check if the column is of numeric type
            if isinstance(dtype, NumericType):
                # Handle missing values: replace with median
                median_value = df.approxQuantile(column, [0.5], 0.0)[0]
                df = df.na.fill({column: median_value})

                # Standard deviation scaling
                mean_val = df.select(mean(col(column))).collect()[0][0]
                stddev_val = df.select(stddev(col(column))).collect()[0][0]
                if stddev_val and stddev_val != 0:
                    df = df.withColumn(column, (col(column) - mean_val) / stddev_val)
                else:
                    logger.warning(f"Standard deviation is zero for column: {column}")
            else:
                # Skip non-numeric columns
                logger.info(f"Skipping non-numeric column: {column}")
        except Exception as e:
            # Log the error and continue with the next column
            logger.error(f"An error occurred while processing column '{column}': {e}")
            logger.info(f"Skipping column '{column}'")
            continue
    return df

# actually perform the preprocessing, take from bronze apply changes, save to silver.
def process_file(file_name, preprocessing_option):
    """Process a file: read from MinIO, transform based on preprocessing option, and write back as a parquet."""
    try:
        if is_file_processed(file_name):  # Check if file has already been processed
            print(f"File {file_name} has already been processed. Skipping...")
            return

        # Read data from MinIO bucket (dw-bucket-bronze)
        input_path = f"s3a://{source_bucket}/{file_name}"

        # Read CSV data into DataFrame
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        print(f"Processing file: {file_name}")

        # Determine and apply transformations based on selected preprocessing option
        if preprocessing_option == "Data Clean Up":
            transformed_df = apply_basic_cleanup(df)
        elif preprocessing_option == "Preprocessing for Machine Learning":
            transformed_df = apply_ml_preprocessing(df)
        else:
            transformed_df = df  # No preprocessing

        transformed_df.show()

        # Define the output path in the bucket and use parquet now instead of IB/Deltatable
        output_file_name = f"{file_name.replace('.csv', '')}_processed.parquet"
        output_path = f"s3a://{destination_bucket}/{output_file_name}"

        # Save the DataFrame as a Parquet file
        transformed_df.write.mode('overwrite').parquet(output_path)

        print(f"Processed and saved file: {file_name} to {destination_bucket}")

        # Mark the file as processed in the metadata bucket
        mark_file_as_processed(file_name)
    except Exception as e:
        print(f"Failed to process file {file_name}: {e}")

def main(file_name, preprocessing_option):
    if file_name.endswith('.csv'):  # Ensure only CSV files are processed
        process_file(file_name, preprocessing_option)
    else:
        print(f"File {file_name} is not a CSV file. Skipping.")

if __name__ == "__main__":
    # Read command-line arguments
    if len(sys.argv) != 3:
        print("Usage: python etl_pipeline.py <file_name> <preprocessing_option>")
        sys.exit(1)
    file_name = sys.argv[1]
    preprocessing_option = sys.argv[2]
    main(file_name, preprocessing_option)
