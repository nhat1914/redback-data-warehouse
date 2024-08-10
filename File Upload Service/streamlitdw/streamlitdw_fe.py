import streamlit as st
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import io
import os

# TEMP Set up MinIO 
minio_client = Minio(
    "10.137.0.149:9000",
    access_key= os.getenv('AWS_ACCESS_KEY_ID'), #"oMRermz5wLf474preMdK", # need to environ these 
    secret_key= os.getenv('AWS_SECRET_ACCESS_KEY'), # need to environ these
    secure=False
)

bucket_name = "file-upload-service-sl"

def upload_to_minio(file, filename):
    try:
        # Convert the uploaded file to bytes
        data = file.read()
        file_stream = io.BytesIO(data)

        # Upload file to file upload service
        minio_client.put_object(
            bucket_name, filename, file_stream, len(data)
        )
        st.success(f"File {filename} uploaded successfully to Data Warehouse.") #try and except block to capture upload issues 
    except S3Error as e:
        st.error(f"Failed to upload {filename} to DataWarehouse: {e}")

def main():
    st.title("File Upload to Redback Data Warehouse Server")

    # File uploader
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "txt", "xlsx","json"])

    if uploaded_file is not None:
        # Display file details
        st.write(f"**Filename:** {uploaded_file.name}")
        st.write(f"**File type:** {uploaded_file.type}")
        st.write(f"**File size:** {uploaded_file.size / (1024 * 1024):.2f} MB")

        # save file option
        if st.button("Upload to Data Warehouse"):
            upload_to_minio(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()