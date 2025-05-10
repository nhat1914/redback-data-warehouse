from bson.objectid import ObjectId
from app import db 
# Get the collection name from environment variables
collection_name = "sales"

class DocumentModel:
    @staticmethod
    def get_all_documents():
        # Access the collection using the name from environment variables
        collection = db[collection_name]
        return list(collection.find({}, {'_id': 0}))

    @staticmethod
    def get_document_by_id(document_id):
        collection = db[collection_name]
        return collection.find_one({"_id": ObjectId(document_id)}, {'_id': 0})

    @staticmethod
    def insert_document(data):
        collection = db[collection_name]
        inserted_id = collection.insert_one(data).inserted_id
        return str(inserted_id)

    @staticmethod
    def update_document(document_id, data):
        collection = db[collection_name]
        result = collection.update_one({"_id": ObjectId(document_id)}, {"$set": data})
        return result.modified_count

    @staticmethod
    def delete_document(document_id):
        collection = db[collection_name]
        result = collection.delete_one({"_id": ObjectId(document_id)})
        return result.deleted_count
