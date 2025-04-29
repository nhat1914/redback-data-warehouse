import streamlit as st
import requests
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import os
import datetime
import subprocess
import pandas as pd
import psycopg2
from elasticsearch import Elasticsearch
import json

# Load environment variables
load_dotenv("dw.env")

# Check the environment variables
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_name = os.getenv('POSTGRES_DB')
minio_address = os.getenv('MINIO_ADDRESS')
api_url_base = os.getenv('API_URL_BASE')

# Check if the env variables are not none before setting them
if access_key is None or secret_key is None:
    raise ValueError("MinIO credentials are empty, these need to be set to continue. Check .env file in virtual machine.")

# Set up MinIO client using the loaded environment variables
minio_client = Minio(
    minio_address,  # MinIO server address
    access_key=access_key,  
    secret_key=secret_key,  
    secure=False  
)
# define buckets
bucket_name_bronze = "dw-bucket-bronze"
bucket_name_silver = "dw-bucket-silver"

def validate_filename(name):
    return name.isalnum()

def generate_custom_filename(project, base_name, original_filename, add_prefix_suffix):
    file_extension = original_filename.split(".")[-1]
    if add_prefix_suffix:
        date_stamp = datetime.datetime.now().strftime("%Y%m%d")
        custom_filename = f"{project}/{base_name}_{date_stamp}.{file_extension}"
    else:
        custom_filename = f"{base_name}.{file_extension}"
    return custom_filename

def upload_to_minio(file, filename, bucket_name, project, preprocessing_option):
    try:
        data = file.read()
        file_stream = io.BytesIO(data)
        minio_client.put_object(bucket_name, filename, file_stream, len(data))
        st.success(f"File {filename} uploaded successfully to {bucket_name}.")

                # For custom_metadata in provenance log
        destination_buckets = ["dw-bucket-bronze"]
        if preprocessing_option != "No Pre-processing":
            destination_buckets = ["dw-bucket-bronze", "dw-bucket-silver"]

        # Log to ELK stack
        log_data = {
            "source": "upload_service",
            "log_level": "INFO",
            "message": "File uploaded successfully",
            "service_name": "file_upload_service",
            "user_id": None,
            "ip_address": None,
            "request_id": None,
            "event_type": "file:uploaded",
            "object_key": filename,
            "object_size": file.size,
            "status": "success",
            "custom_metadata": {
                "original_filename": file.name,
                "updated_filename": filename,
                "preprocessing_option": preprocessing_option,
                "destination_buckets": destination_buckets,
                "project": project
            }
        }
        log_to_elasticsearch(log_data)

        # Log provenance data to provenance db
        log_provenance(
            event_source='upload:service',
            event_type='file:uploaded',
            user_id=None,  # Upload service has no log in as of 26/11/2024
            source_ip=None,
            bucket_name=None,
            object_key=None,
            object_size=file.size,
            object_etag=None,  # Missing value
            content_type=file.type,
            service_endpoint='http://0.0.0.0:80',
            custom_metadata={
                "original_filename": file.name,
                "updated_filename": filename,
                "preprocessing_option": preprocessing_option,
                "destination_buckets": destination_buckets,
                "project": project
            }
        )
    except S3Error as e:
        st.error(f"Failed to upload {filename} to {bucket_name}: {e}")

def log_to_elasticsearch(log_data):
    try:
        response = requests.post("http://dp-logstash:5044", json=log_data)
        if response.status_code != 200:
            print(f"Failed to log to Logstash: {response.text}")
    except Exception as e:
        print(f"Failed to log to Logstash: {e}")

def log_provenance(event_source, event_type, user_id, source_ip, bucket_name, object_key, object_size, object_etag, content_type, service_endpoint, custom_metadata=None):
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host
        )
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO provenance (
                event_source,
                event_type,
                user_id,
                source_ip,
                bucket_name,
                object_key,
                object_size,
                object_etag,
                content_type,
                service_endpoint,
                custom_metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event_source,
                event_type,
                user_id,
                source_ip,
                bucket_name,
                object_key,
                object_size,
                object_etag,
                content_type,
                service_endpoint,
                json.dumps(custom_metadata) if custom_metadata else None
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Failed to log provenance data: {e}")

def trigger_etl(file_name, preprocessing_option):
    """Trigger the ETL pipeline with the selected preprocessing option."""
    try:
        result = subprocess.run(
            ["python", "etl_pipeline.py", file_name, preprocessing_option], 
            check=True, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        st.success("ETL pipeline executed successfully.")
        st.text(f"ETL Output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to execute ETL pipeline: {e}")
        st.text(f"ETL Error Output: {e.stderr}")

def get_file_list(bucket):
    try:
        # this is the flask api to access the list of data back out of the VM
        api_url = f"http://{api_url_base}/list-files?bucket={bucket}"
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to retrieve file list from {bucket}.")
            return {}
    except Exception as e:
        st.error(f"Error retrieving file list from {bucket}: {e}") # added logs because of annoying errors
        return {}

def download_file(bucket, project, filename):
    try:
        api_url = f"http://{api_url_base}/download-file"
        params = {"bucket": bucket, "project": project, "filename": filename}  # Avoid re-adding the project folder
        response = requests.get(api_url, params=params)
        st.write(f"API URL: {api_url}, Params: {params}, Status Code: {response.status_code}")  # added logs
        if response.status_code == 200:
            # Log to ELK stack
            log_data = {
                "source": "upload_service",
                "log_level": "INFO",
                "message": "File downloaded successfully",
                "service_name": "file_upload_service",
                "user_id": None,
                "ip_address": None,
                "request_id": None,
                "event_type": "file:downloaded",
                "object_key": filename,
                "object_size": None,
                "status": "success",
                "custom_metadata": {
                    "project": project,
                    "bucket": bucket,
                }
            }
            log_to_elasticsearch(log_data)
            return response.content
        else:
            st.error(f"Failed to download file from {bucket}. Status Code: {response.status_code}, Error: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error downloading file from {bucket}: {e}")
        return None

def main():
    st.title("File Upload and Download for Redback Data Warehouse")

    # Create tabs for File Upload, Bronze, and Silver
    tabs = st.tabs(["File Upload & ETL", "View Original Files", "View Pre-processed Files"])

    # Tab 1: File Upload & ETL
    with tabs[0]:
        st.header("File Upload Section")

        # Project selection dropdown
        project = st.selectbox("Select Project", options=["project1", "project2", "project3", "project4", "project5", "other"], key="upload_project")

        # File uploader with expanded file types
        uploaded_file = st.file_uploader(
        "Choose a file", 
        type=["csv", "txt", "xlsx", "json", "mp4", "jpg", "jpeg", "png"]
        )

        # Preprocessing selection dropdown
        preprocessing_option = st.selectbox(
            "Preprocessing (optional)",
            options=["No Pre-processing", "Data Clean Up", "Preprocessing for Machine Learning"],
            help="Choose a preprocessing option for the uploaded data.",
            key="preprocessing_option"
        )

        # box for enabling/disabling prefix and suffix
        add_prefix_suffix = st.checkbox("Add project as prefix and date as suffix to filename (to overwrite existing files)", value=True)

        if uploaded_file is not None:
            base_name = st.text_input("Enter base name for the file:")

            # Display file details
            st.write(f"**Uploaded File Name:** {uploaded_file.name}")
            st.write(f"**File Type:** {uploaded_file.type}")
            st.write(f"**File Size:** {uploaded_file.size / (1024 * 1024):.2f} MB")

            # Check and validate filename
            if base_name and validate_filename(base_name):
                # Generate the custom filename with the project prefix
                custom_filename = generate_custom_filename(
                    project, base_name, uploaded_file.name, add_prefix_suffix
                )
                st.write(f"**Generated Filename:** {custom_filename}")

                if st.button("Upload to Data Warehouse"):
                    # Upload raw file to Bronze
                    upload_to_minio(
                        uploaded_file, custom_filename, bucket_name_bronze, project, preprocessing_option
                    )
                    st.write(f"Selected Preprocessing Option: {preprocessing_option}")
                    
                    # Trigger ETL pipeline if applicable
                    if preprocessing_option != "No Pre-processing":
                        trigger_etl(custom_filename, preprocessing_option)
                    
            else:
                st.warning("Please enter a valid base name. Only alphanumeric characters are allowed.")

    # Tab 2: View Bronze Files
    with tabs[1]:
        st.header("Uploaded Files Overview - Bronze (dw-bucket-bronze)")

        # Get the list of files from the "dw-bucket-bronze" bucket
        files_by_project = get_file_list("dw-bucket-bronze")

        if files_by_project:
            available_projects = ["Select a project"] + list(files_by_project.keys())  # Add default option
            selected_project = st.selectbox("Select Project Folder", available_projects, key="bronze_project")

            if selected_project != "Select a project" and selected_project in files_by_project:
                file_list = [{"Project": selected_project, "File": file} for file in files_by_project[selected_project]]

                if file_list:
                    df = pd.DataFrame(file_list)
                    st.dataframe(df)  # Display the table with the filtered list of files

                    file_options = ["Select a file"] + df["File"].tolist()  # Add default option
                    selected_file = st.selectbox("Select File to Download", file_options, key="bronze_file")

                    if selected_file != "Select a file":
                        file_content = download_file("dw-bucket-bronze", selected_project, selected_file)
                        if file_content:
                            st.download_button(
                                label="Download File",
                                data=file_content,
                                file_name=selected_file.split("/")[-1],
                                mime="application/octet-stream"
                            )

    # Tab 3: View Silver Files
    with tabs[2]:
        st.header("Uploaded Files Overview - Silver (dw-bucket-silver)")

        # Get the list of files from the "dw-bucket-silver" bucket
        files_by_project = get_file_list("dw-bucket-silver")

        if files_by_project:
            available_projects = ["Select a project"] + list(files_by_project.keys())  # Add default option
            selected_project = st.selectbox("Select Project Folder", available_projects, key="silver_project")

            if selected_project != "Select a project" and selected_project in files_by_project:
                file_list = [{"Project": selected_project, "File": file} for file in files_by_project[selected_project]]

                if file_list:
                    df = pd.DataFrame(file_list)
                    st.dataframe(df)  # Display the table with the filtered list of files

                    file_options = ["Select a file"] + df["File"].tolist()  # Add default option
                    selected_file = st.selectbox("Select File to Download", file_options, key="silver_file")

                    if selected_file != "Select a file":
                        file_content = download_file("dw-bucket-silver", selected_project, selected_file)
                        if file_content:
                            st.download_button(
                                label="Download File",
                                data=file_content,
                                file_name=selected_file.split("/")[-1],
                                mime="application/octet-stream"
                            )

if __name__ == "__main__":
    main()
