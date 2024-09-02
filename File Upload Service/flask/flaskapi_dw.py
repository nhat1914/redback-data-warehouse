from flask import Flask, jsonify, send_file, Response
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os
import io

app = Flask(__name__)

# minio details
MINIO_URL = "10.137.0.149:9000"
ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID') 
SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY') 
BUCKET_NAME = "file-upload-service-sl"

# Initialise Mini
minio_client = Minio(
    MINIO_URL,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=False
)

# Endpoint to list files in the bucket
@app.route('/list-files', methods=['GET'])
def list_files():
    try:
        objects = minio_client.list_objects(BUCKET_NAME)
        files = [obj.object_name for obj in objects]
        return jsonify(files)
    except S3Error as err:
        return jsonify({"error": str(err)}), 500

# Endpoint to download a file from the bucket
@app.route('/download-file/<filename>', methods=['GET'])
def download_file(filename):
    try:
        data = minio_client.get_object(BUCKET_NAME, filename)
        return send_file(
            io.BytesIO(data.read()),
            attachment_filename=filename,
            as_attachment=True
        )
    except S3Error as err:
        return jsonify({"error": str(err)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) # runnning on 5000