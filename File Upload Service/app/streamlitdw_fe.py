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

# Load environment variables
load_dotenv()

# Check the environment variables
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Check if the env variables are not none before setting them
if access_key is None or secret_key is None:
    raise ValueError("MinIO credentials are empty, these need to be set to continue. Check .env file in virtual machine.")

# Set up MinIO client using the loaded environment variables
minio_client = Minio(
    "10.137.0.149:9000",  # MinIO server address
    access_key=access_key,  
    secret_key=secret_key,  
    secure=False  
)
# define buckets
bucket_name_bronze = "dw-bucket-bronze"
bucket_name_silver = "dw-bucket-silver"

def validate_filename(name):
    return name.isalnum()
# generate custom filename with suffix and prefix to enforce a bit of governance 
def generate_custom_filename(project, base_name, original_filename, add_prefix_suffix):
    file_extension = original_filename.split(".")[-1]
    if add_prefix_suffix:
        date_stamp = datetime.datetime.now().strftime("%Y%m%d")
        custom_filename = f"{project}/{base_name}_{date_stamp}.{file_extension}"
    else:
        custom_filename = f"{base_name}.{file_extension}"
    return custom_filename

def upload_to_minio(file, filename, bucket_name):
    try:
        data = file.read()
        file_stream = io.BytesIO(data)
        minio_client.put_object(bucket_name, filename, file_stream, len(data))
        st.success(f"File {filename} uploaded successfully to {bucket_name}.")
    except S3Error as e:
        st.error(f"Failed to upload {filename} to {bucket_name}: {e}")

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
        api_url = f"http://10.137.0.149:5000/list-files?bucket={bucket}"  # Updated
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to retrieve file list from {bucket}.")
            return {}
    except Exception as e:
        st.error(f"Error retrieving file list from {bucket}: {e}") # added logs because of annoying errors
        return {}

# Function to download file using Flask API using flaskapi_dw.py
def download_file(bucket, project, filename):
    try:
        
        api_url = f"http://10.137.0.149:5000/download-file"
        params = {"bucket": bucket, "project": project, "filename": filename}  # Avoid re-adding the project folder
        response = requests.get(api_url, params=params)
        st.write(f"API URL: {api_url}, Params: {params}, Status Code: {response.status_code}")  # added logs
        if response.status_code == 200:
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
        project = st.selectbox("Select Project", options=["project1", "project2", "project3", "project4", "project5", "other"])

        # File uploader
        uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt", "xlsx", "json"])

        # Preprocessing selection dropdown
        preprocessing_option = st.selectbox(
            "Preprocessing (optional)",
            options=["No Pre-processing", "Data Clean Up", "Preprocessing for Machine Learning"],
            help="Choose a preprocessing option for the uploaded data."
        )

        # box for enabling/disabling prefix and suffix
        add_prefix_suffix = st.checkbox("Add project as prefix and date as suffix to filename (to overwrite existing files)", value=True)


        if uploaded_file is not None:
            base_name = st.text_input("Enter base name for the file:")

            if base_name and validate_filename(base_name):
                # Generate the custom filename with the project prefix
                custom_filename = generate_custom_filename(project, base_name, uploaded_file.name, add_prefix_suffix)
                # Display file details
                st.write(f"**Filename:** {custom_filename}")
                st.write(f"**File type:** {uploaded_file.type}")
                st.write(f"**File size:** {uploaded_file.size / (1024 * 1024):.2f} MB")

                if st.button("Upload to Data Warehouse"):
                    # Upload raw file to Bronze
                    upload_to_minio(uploaded_file, custom_filename, bucket_name_bronze)
                    st.write(f"Selected preprocessing option: {preprocessing_option}")
                    # Trigger ETL pipeline with the selected preprocessing option
                    trigger_etl(custom_filename, preprocessing_option)
            else:
                st.warning("Please enter a valid base name. Only alphanumeric characters are allowed.")

    # Tab 2: View Bronze Files
    with tabs[1]:
        st.header("Uploaded Files Overview - Bronze (dw-bucket-bronze)")

        # Get the list of files from the "dw-bucket-bronze" bucket
        files_by_project = get_file_list("dw-bucket-bronze")

        if files_by_project:
            available_projects = list(files_by_project.keys())  # Get project names (folders)
            selected_project = st.selectbox("Select Project Folder", available_projects)

            if selected_project in files_by_project:
                file_list = [{"Project": selected_project, "File": file} for file in files_by_project[selected_project]]

                if file_list:
                    df = pd.DataFrame(file_list)
                    st.dataframe(df)  # Display the table with the filtered list of files

                    selected_file = st.selectbox("Select File to Download", df["File"].tolist())

                    if st.button("Download Selected File from Bronze"):
                        file_content = download_file("dw-bucket-bronze", selected_project, selected_file)
                        if file_content:
                            st.download_button(label=f"Download {selected_file}", data=file_content, file_name=selected_file.split("/")[-1])

    # Tab 3: View Silver Files
    with tabs[2]:
        st.header("Uploaded Files Overview - Silver (dw-bucket-silver)")

        # Get the list of files from the "dw-bucket-silver" bucket
        files_by_project = get_file_list("dw-bucket-silver")

        if files_by_project:
            available_projects = list(files_by_project.keys())  # Get project names (folders)
            selected_project = st.selectbox("Select Project Folder", available_projects)

            if selected_project in files_by_project:
                file_list = [{"Project": selected_project, "File": file} for file in files_by_project[selected_project]]

                if file_list:
                    df = pd.DataFrame(file_list)
                    st.dataframe(df)  # Display the table with the filtered list of files

                    selected_file = st.selectbox("Select File to Download", df["File"].tolist())

                    if st.button("Download Selected File from Silver"):
                        file_content = download_file("dw-bucket-silver", selected_project, selected_file)
                        if file_content:
                            st.download_button(label=f"Download {selected_file}", data=file_content, file_name=selected_file.split("/")[-1])

if __name__ == "__main__":
    main()