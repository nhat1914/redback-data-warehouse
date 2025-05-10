import os
#from dotenv import load_dotenv
#load_dotenv()
class Config:
    # Fetch MongoDB URI and database name from environment variables
    MONGO_URI = os.environ.get('MONGO_URI')
    DB_NAME = os.environ.get('DB_NAME')

    # Raise an error if the required environment variables are not set
    if not MONGO_URI or not DB_NAME:
        raise ValueError("Required environment variables MONGO_URI and DB_NAME are not set")