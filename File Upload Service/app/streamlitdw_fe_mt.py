import streamlit as st
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import os
import datetime
import subprocess  # For triggering ETL pipeline
import pandas as pd  # Added for dataframe functionality

# Load environment variables
load_dotenv()

# Check the environment variables 
access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Set up MinIO client using the loaded environment variables
minio_client = Minio(
    "10.137.0.149:9000",  # MinIO server address
    access_key=access_key,  
    secret_key=secret_key,  
    secure=False  
)

bucket_name_bronze = "dw-bucket-bronze"

def validate_filename(name):
    # Ensure the filename is alphanumeric. Possibly may add rules here.
    return name.isalnum()

def generate_custom_filename(project, base_name, original_filename):
    # Extract file extension
    file_extension = original_filename.split(".")[-1]
    # Generate a custom name with the project prefix, base_name, and a date (YYYYMMDD)
    date_stamp = datetime.datetime.now().strftime("%Y%m%d")
    custom_filename = f"{project}/{base_name}_{date_stamp}.{file_extension}"  # Use project as folder prefix
    return custom_filename

def upload_to_minio(file, filename, bucket_name):
    try:
        # Convert the uploaded file to bytes
        data = file.read()
        file_stream = io.BytesIO(data)

        # Upload file to MinIO, using filename with the project prefix as the object name
        minio_client.put_object(
            bucket_name, filename, file_stream, len(data)
        )
        st.success(f"File {filename} uploaded successfully to Data Warehouse.")
    except S3Error as e:
        st.error(f"Failed to upload {filename} to Data Warehouse: {e}")

def trigger_etl(preprocessing_option):
    """Trigger the ETL pipeline with the selected preprocessing option."""
    try:
        # Run the ETL script as a subprocess
        subprocess.run(["python", "etl_pipeline.py", preprocessing_option], check=True)
        st.success("ETL pipeline executed successfully.")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to execute ETL pipeline: {e}")

def load_sample_data():
    # Example DataFrame, replace this with your own logic for loading data from a CSV, database, or MinIO
    data = {
        'Name': ['Project 1', 'Project 2', 'Project 3'],
        'Files Uploaded': [12, 8, 23],
        'Last Upload': ['2023-09-10', '2023-09-14', '2023-09-16']
    }
    df = pd.DataFrame(data)
    return df

def main():
    st.title("File Upload to Redback Data Warehouse Server")

    # Setup for multiple tabs
    tabs = st.tabs(["File Upload", "View Uploaded Files", "Tab 3"])  # Updated tab names as needed
    
    # Tab 1: File Upload
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

        if uploaded_file is not None:
            base_name = st.text_input("Enter base name for the file:")

            if base_name and validate_filename(base_name):
                # Generate the custom filename with the project prefix
                custom_filename = generate_custom_filename(project, base_name, uploaded_file.name)
                # Display file details
                st.write(f"**Filename:** {custom_filename}")
                st.write(f"**File type:** {uploaded_file.type}")
                st.write(f"**File size:** {uploaded_file.size / (1024 * 1024):.2f} MB")

                if st.button("Upload to Data Warehouse"):
                    # Upload raw file to Bronze
                    upload_to_minio(uploaded_file, custom_filename, bucket_name_bronze)
                    # Display selected preprocessing option
                    st.write(f"Selected preprocessing option: {preprocessing_option}")
                    # Trigger ETL pipeline with the selected preprocessing option
                    trigger_etl(preprocessing_option)
            else:
                st.warning("Please enter a valid base name. Only alphanumeric characters are allowed.")

    # Tab 2: View Uploaded Files (DataFrame display)
    with tabs[1]:
        st.header("Uploaded Files Overview")
        # Load and display sample DataFrame
        df = load_sample_data()  # You can replace this with actual data fetching logic
        st.dataframe(df)  # Display DataFrame as a table

    # Tab 3: Placeholder for additional functionalities (e.g., view uploaded files)
    with tabs[2]:
        st.header("Tab 3: Additional Functionality")
        st.write("Add content for Tab 3 here.")        

if __name__ == "__main__":
    main()
