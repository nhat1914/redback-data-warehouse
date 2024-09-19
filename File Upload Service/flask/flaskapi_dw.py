from flask import Flask, jsonify, send_file, request
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os
import io

app = Flask(__name__)

# Load environment variables
load_dotenv()

# MinIO details - loading credentials for Bronze from the .env file
MINIO_URL = "10.137.0.149:9000"
BRONZE_ACCESS_KEY = os.getenv('BRONZE_ACCESS_KEY')  # Access key from the .env file
BRONZE_SECRET_KEY = os.getenv('BRONZE_SECRET_KEY')  # Secret key from the .env file

# Initialize MinIO with credentials from the .env file
minio_client = Minio(
    MINIO_URL,
    access_key=BRONZE_ACCESS_KEY,
    secret_key=BRONZE_SECRET_KEY,
    secure=False
)

# Debug testing code
@app.route('/debug', methods=['GET'])
def debug():
    return jsonify({
        "ACCESS_KEY": os.getenv('BRONZE_ACCESS_KEY'),
        "SECRET_KEY": os.getenv('BRONZE_SECRET_KEY')
    })


# Endpoint to list files in the specified bucket (either Bronze or Silver)
@app.route('/list-files', methods=['GET'])
def list_files():
    bucket_name = request.args.get('bucket')
    
    # Validate bucket name (Bronze and Silver only)
    if bucket_name not in ['dw-bucket-bronze', 'dw-bucket-silver']:
        return jsonify({"error": "Invalid bucket name"}), 400

    try:
        # Dictionary to hold files grouped by project
        files_by_project = {}
        objects = minio_client.list_objects(bucket_name, recursive=True)
        
        has_files = False
        for obj in objects:
            has_files = True
            folder_name = obj.object_name.split('/')[0]  # Extract the project name
            if folder_name not in files_by_project:
                files_by_project[folder_name] = []
            files_by_project[folder_name].append(obj.object_name)

        if not has_files:
            return jsonify({"message": f"No files found in bucket {bucket_name}"}), 200

        return jsonify(files_by_project)

    except S3Error as err:
        return jsonify({"error": str(err)}), 500

# Endpoint to download a file from the specified bucket
@app.route('/download-file', methods=['GET'])
def download_file():
    bucket_name = request.args.get('bucket')
    project = request.args.get('project')
    filename = request.args.get('filename')
    
    # Validate request parameters
    if bucket_name not in ['dw-bucket-bronze', 'dw-bucket-silver'] or not project or not filename:
        return jsonify({"error": "Please provide valid 'bucket', 'project', and 'filename' parameters."}), 400
    
    file_path = f"{project}/{filename}"
    
    try:
        data = minio_client.get_object(bucket_name, file_path)
        return send_file(
            io.BytesIO(data.read()),
            download_name=filename,
            as_attachment=True
        )
    except S3Error as err:
        return jsonify({"error": str(err)}), 500
    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)  # Running on port 5000