from flask import Flask, jsonify, send_file, Response, request
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os
import io

app = Flask(__name__)

# Load environment variables
load_dotenv()

# MinIO details
MINIO_URL = "10.137.0.149:9000"
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID') 
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
BUCKET_NAME = "file-upload-service-sl"

# Initialize MinIO
minio_client = Minio(
    MINIO_URL,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# Endpoint to list files in the bucket, grouped by project
@app.route('/list-files', methods=['GET'])
def list_files():
    try:
        # Dictionary to hold files grouped by project 
        files_by_project = {}
        objects = minio_client.list_objects(BUCKET_NAME, recursive=True)
        for obj in objects:
            folder_name = obj.object_name.split('/')[0]  # Extract the project name
            if folder_name not in files_by_project:
                files_by_project[folder_name] = []
            files_by_project[folder_name].append(obj.object_name)
        return jsonify(files_by_project)
    except S3Error as err:
        return jsonify({"error": str(err)}), 500

# Endpoint to download a file from the bucket, specifying project and filename
@app.route('/download-file', methods=['GET'])
def download_file():
    project = request.args.get('project')
    filename = request.args.get('filename')
    
    if not project or not filename:
        return jsonify({"error": "Please provide both 'project' and 'filename' parameters."}), 400
    
    file_path = f"{project}/{filename}"
    
    try:
        data = minio_client.get_object(BUCKET_NAME, file_path)
        return send_file(
            io.BytesIO(data.read()),
            download_name=filename,
            as_attachment=True
        )
    except S3Error as err:
        return jsonify({"error": str(err)}), 500

# Add in the metadata code to specify the file when requesting
# This function below presents the files and lists them in their respective folders
@app.route('/file-metadata', methods=['GET'])
def file_metadata():
    project = request.args.get('project')
    filename = request.args.get('filename')
    
    if not project or not filename:
        return jsonify({"error": "Please provide both 'project' and 'filename' parameters."}), 400

    file_path = f"{project}/{filename}"
    
    try:
        stat = minio_client.stat_object(BUCKET_NAME, file_path)
        metadata = {
            "filename": filename,
            "size": stat.size,
            "last_modified": stat.last_modified.isoformat(),
            "etag": stat.etag,
            "content_type": stat.content_type
        }
        return jsonify(metadata)
    except S3Error as err:
        return jsonify({"error": str(err)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)  # Running on port 5000
