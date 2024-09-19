from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, mean, stddev, lit, monotonically_increasing_id
from minio import Minio
from minio.error import S3Error
import os
import io  # Import for handling byte streams
from datetime import datetime

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
    .config("spark.hadoop.fs.s3a.endpoint", "http://10.137.0.149:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
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
    """Basic data clean up: remove nulls, duplicates, blank columns, add extract date, and unique ID."""
    print("Applying basic data clean up...")
    
    # Remove columns that are entirely blank, null or empty
    non_blank_columns = [col_name for col_name in df.columns if df.filter(col(col_name).isNotNull() & (col(col_name) != "")).count() > 0]
    df = df.select(non_blank_columns)

    # Remove rows with nulls and duplicates
    df = df.dropna()  # Drop rows with null values
    df = df.dropDuplicates()  # Remove duplicate rows
    
    # Add extract date column
    extract_date = datetime.now().strftime('%Y-%m-%d')
    df = df.withColumn("extract_date", lit(extract_date))
    
    # Add unique ID column
    df = df.withColumn("unique_id", monotonically_increasing_id())
    
    return df

# Preprocessing option 2
def apply_ml_preprocessing(df):
    """Preprocessing for Machine Learning: fill missing values, scale numeric features
        The ML preprocessing aims to pre-perform some of the fundamental changes required to perform ML
        this function detects datatypes that are able to """
    print("Applying preprocessing for Machine Learning...")
    for column in df.columns:
        # Handle missing values: replace with median
        if df.schema[column].dataType.simpleString() in ["double", "int", "float", "long"]:
            median_value = df.approxQuantile(column, [0.5], 0.0)[0]
            df = df.na.fill({column: median_value})

        # standard deviation scaling
        mean_val = df.select(mean(col(column))).collect()[0][0]
        stddev_val = df.select(stddev(col(column))).collect()[0][0]
        if stddev_val and stddev_val != 0:
            df = df.withColumn(column, (col(column) - mean_val) / stddev_val)

    return df

# actually perform the preprocessing, take from bronze apply changes, save to silver.
def process_file(file_name, preprocessing_option):
    """Process a file: read from MinIO, transform based on preprocessing option, and write back as a Iceberg table."""
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

        # Define the output path in the bucket and use parquet now instead of IB
        output_file_name = f"{file_name.replace('.csv', '')}_processed.parquet"
        output_path = f"s3a://{destination_bucket}/{output_file_name}"

        # Save the DataFrame as a Parquet file
        transformed_df.write.mode('overwrite').parquet(output_path)

        print(f"Processed and saved file: {file_name} to {destination_bucket}")

        # Mark the file as processed in the metadata bucket
        mark_file_as_processed(file_name)
    except Exception as e:
        print(f"Failed to process file {file_name}: {e}")

def main(preprocessing_option):
    # List all files in the 'dw-bucket-bronze' bucket
    files_to_process = list_files_in_bucket(source_bucket)

    # Process each file dynamically
    for file_name in files_to_process:
        if file_name.endswith('.csv'):  # Ensure only CSV files are processed
            process_file(file_name, preprocessing_option)

if __name__ == "__main__":
    # Example call to main function with selected preprocessing option
    selected_preprocessing_option = "Data Clean Up"  # Replace with selected option
    main(selected_preprocessing_option)
