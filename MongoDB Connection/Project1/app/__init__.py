from flask import Flask
from pymongo import MongoClient
import os
import logging

# Configure logging
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app.log')
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load configuration
app.config.from_object('config.Config')

# MongoDB connection setup
try:
    client = MongoClient(app.config['MONGO_URI'])
    db = client[app.config['DB_NAME']]
    logger.info("Connected to MongoDB successfully!")
except Exception as e:
    logger.error("Failed to connect to MongoDB: {}".format(e))
    raise

# Import controllers to register routes
from app.controllers import document_controller