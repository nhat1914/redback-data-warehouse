from flask import jsonify, request
from app import app
from app.models.document_model import DocumentModel

@app.route('/documents', methods=['GET'])
def get_documents():
    documents = DocumentModel.get_all_documents()
    return jsonify(documents), 200

@app.route('/documents/<document_id>', methods=['GET'])
def get_document(document_id):
    document = DocumentModel.get_document_by_id(document_id)
    if document:
        return jsonify(document), 200
    else:
        return jsonify({"error": "Document not found"}), 404

@app.route('/documents', methods=['POST'])
def insert_document():
    data = request.json
    document_id = DocumentModel.insert_document(data)
    return jsonify({"message": "Document inserted successfully!", "id": document_id}), 201

@app.route('/documents/<document_id>', methods=['PUT'])
def update_document(document_id):
    data = request.json
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
