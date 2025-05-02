from flask import request, jsonify
from bson import ObjectId
from app import app
from app.models.document_model import DocumentModel

def validate_document(data):
    # Modify this validation logic as needed based on your team requirements
    required_fields = ["title", "content"]
    for field in required_fields:
        if field not in data or not isinstance(data[field], str):
            return False

    # Example: Check if 'id' field is a valid ObjectId if it exists
    if 'id' in data and not ObjectId.is_valid(data['id']):
        return False

    return True

@app.route('/documents', methods=['POST'])
def insert_document():
    data = request.json
    if not validate_document(data):
        return jsonify({"error": "Invalid document data"}), 400
    document_id = DocumentModel.insert_document(data)
    return jsonify({"message": "Document inserted successfully!", "id": document_id}), 201

@app.route('/documents/<document_id>', methods=['PUT'])
def update_document(document_id):
    data = request.json
    if not validate_document(data):
        return jsonify({"error": "Invalid document data"}), 400
    updated_count = DocumentModel.update_document(document_id, data)
    if updated_count > 0:
        return jsonify({"message": "Document updated successfully!"}), 200
    else:
        return jsonify({"error": "Document not found or no changes made"}), 404

@app.route('/documents/<document_id>', methods=['DELETE'])
def delete_document(document_id):
    deleted_count = DocumentModel.delete_document(document_id)
    if deleted_count > 0:
        return jsonify({"message": "Document deleted successfully!"}), 200
    else:
        return jsonify({"error": "Document not found"}), 404

@app.route('/documents/<document_id>', methods=['GET'])
def get_document_by_id(document_id):
    if not ObjectId.is_valid(document_id):
        return jsonify({"error": "Invalid document ID"}), 400
    document = DocumentModel.get_document_by_id(document_id)
    if document:
        return jsonify(document), 200
    else:
        return jsonify({"error": "Document not found"}), 404

@app.route('/documents', methods=['GET'])
def get_all_documents():
    documents = DocumentModel.get_all_documents()
    return jsonify(documents), 200