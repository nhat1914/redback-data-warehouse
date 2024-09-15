import streamlit as st
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import os
import datetime

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

bucket_name = "file-upload-service-sl"

def validate_filename(name):
    # Ensure the the filename is alphanumeric. Possibly may add rules here.
    return name.isalnum()

def generate_custom_filename(project, base_name, original_filename):
    # Extract file extension
    file_extension = original_filename.split(".")[-1]
    # Generate a custom name with the project prefix, base name, and a date (YYYYMMDD)
    date_stamp = datetime.datetime.now().strftime("%Y%m%d")
    custom_filename = f"{project}/{base_name}_{date_stamp}.{file_extension}"  # Use project as folder prefix
    return custom_filename

def upload_to_minio(file, filename):
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

def main():
    st.title("File Upload to Redback Data Warehouse Server")

    # Project selection dropdown
    project = st.selectbox("Select Project", options=["project1", "project2", "project3", "project4", "project5", "other"])

    # File uploader
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt", "xlsx", "json"])

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
                upload_to_minio(uploaded_file, custom_filename)
        else:
            st.warning("Please enter a valid base name. Only alphanumeric characters are allowed.")

if __name__ == "__main__":
    main()