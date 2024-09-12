from flask import Flask
from pymongo import MongoClient


app = Flask(__name__)

# Load configuration
app.config.from_object('config.Config')

# MongoDB connection setup
try:
    client = MongoClient(app.config['MONGO_URI'])
    db = client[app.config['DB_NAME']]
    print("Connected to MongoDB successfully!")
except Exception as e:
    print("Failed to connect to MongoDB:", e)

# Import controllers to register routes
from app.controllers import document_controller
