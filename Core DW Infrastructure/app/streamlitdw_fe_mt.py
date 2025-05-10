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

# Set up MinIO client
minio_client = Minio(
    "10.137.0.149:9000",   # Minio Server address
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

# define buckets
bucket_name_bronze = "dw-bucket-bronze"
bucket_name_silver = "dw-bucket-silver"

def validate_filename(name):
    return name.isalnum()

# Generate custom filename with suffix and prefix to enforce governance
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


def trigger_etl(filename, preprocessing_option):
    """Trigger the ETL pipeline with the selected preprocessing option."""
    try:
        result = subprocess.run(
            ["python", "etl_pipeline.py", filename, preprocessing_option],
            check=True,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True
        )
        st.success(f"ETL pipeline executed successfully for: {filename}")
        st.text(result.stdout)
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to execute ETL pipeline for: {filename}")
        st.text(f"ETL Error Output: {e.stderr}")


def get_file_list(bucket):
    try:
        # Flask API to access the list of data from the VM
        api_url = f"http://10.137.0.149:5000/list-files?bucket={bucket}"
        response = requests.get(api_url)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to retrieve file list from {bucket}.")
            return {}
    except Exception as e:
        st.error(f"Error retrieving file list from {bucket}: {e}")
        return {}

# Function to download file using Flask API using flaskapi_dw.py
def download_file(bucket, project, filename):
    try:
        api_url = f"http://10.137.0.149:5000/download-file"
        params = {"bucket": bucket, "project": project, "filename": filename}
        response = requests.get(api_url, params=params)
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

    # Initialize session state
    if "uploaded_filenames" not in st.session_state:
        st.session_state.uploaded_filenames = []

    
    # Create tabs for File Upload, Bronze, and Silver
    tabs = st.tabs(["File Upload & ETL", "View Original Files", "View Pre-processed Files"])

    #  Tab 1: File Upload & ETL
    with tabs[0]:
        st.header("File Upload Section")

        project = st.selectbox("Select Project", options=["project1", "project2", "project3", "project4", "project5","other"])
        num_files = st.number_input("Number of files to upload", 1, 10, 1)
        preprocessing = st.selectbox("Preprocessing (optional)", options=["No Pre-processing", "Data Clean Up", "Preprocessing for Machine Learning"])
        add_prefix = st.checkbox("Add project as prefix and date as suffix to filename (to overwrite existing files)", value=True)

        uploaded_files = []
        base_names = []
        valid_basenames = True
        
        with st.container():
            for i in range(num_files):
                file = st.file_uploader(f"File {i + 1}", type=["csv", "txt", "json", "xlsx"], key=f"file_{i}")
                if file:
                    uploaded_files.append(file)
                    default_base = file.name.rsplit('.', 1)[0]
                    base = st.text_input(f"Base name for {file.name}", value=default_base, key=f"base_{i}")
                    if not validate_filename(base):
                        st.warning(f"Base name for {file.name} must be alphanumeric.")
                        valid_basenames = False
                    base_names.append(base)
                else:
                    base_names.append(None)

        # Cleanly outside the container block
        st.markdown("---")
        if st.button("Upload Files"):

            if not uploaded_files:
                st.warning("Please select at least one file.")
            elif not valid_basenames:
                st.warning("Please fix invalid base names.")
            else:
                st.session_state.uploaded_filenames = []
                for idx, file in enumerate(uploaded_files):
                    custom_name = generate_custom_filename(project, base_names[idx], file.name, add_prefix)
                    upload_to_minio(file, custom_name, bucket_name_bronze)
                    st.session_state.uploaded_filenames.append(custom_name)

        # Option to trigger ETL after all uploads
        if st.session_state.uploaded_filenames:
            if st.button("Triggering ETL for All Uploaded Files"):
                for filename in st.session_state.uploaded_filenames:
                    trigger_etl(filename, preprocessing)
        
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
