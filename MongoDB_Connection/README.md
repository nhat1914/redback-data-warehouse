# Project1 Web Server

This project is a web server application that connects to a MongoDB database. The setup uses Docker Compose to manage the services.

## Prerequisites

- Docker
- Docker Compose

## Setup

### 1. Clone the Repository

```sh
git clone https://github.com/BeniDage/project1-data-team.git

cd Project1

```

### 2. Run Docker Compose to build the images and run the services:

```bash
- docker-compose up --build
```

### 3. View the Application

- Open your browser and navigate to http://localhost:5003/

### Changing MongoDB Documents and Collections

- config.py contains the MongoDB connection string.
- document_model.py contains the MongoDB collection name.

## API Endpoints

### 1. Get All Documents

- **Endpoint**: `/documents`
- **Method**: `GET`
- **Description**: Retrieves all documents from the database.
- **Response**:
  - `200 OK`: Returns a JSON array of documents.

### 2. Get Document by ID

- **Endpoint**: `/documents/<document_id>`
- **Method**: `GET`
- **Description**: Retrieves a document by its ID.
- **Parameters**:
  - `document_id` (path): The ID of the document to retrieve.
- **Response**:
  - `200 OK`: Returns the document as a JSON object.
  - `404 Not Found`: If the document is not found.

### 3. Insert Document

- **Endpoint**: `/documents`
- **Method**: `POST`
- **Description**: Inserts a new document into the database.
- **Request Body**: JSON object representing the document to insert.
- **Response**:
  - `201 Created`: Returns a success message and the ID of the inserted document.

### 4. Update Document

- **Endpoint**: `/documents/<document_id>`
- **Method**: `PUT`
- **Description**: Updates an existing document by its ID.
- **Parameters**:
  - `document_id` (path): The ID of the document to update.
- **Request Body**: JSON object representing the updated document data.
- **Response**:
  - `200 OK`: Returns a success message if the document was updated.
  - `404 Not Found`: If the document is not found or no changes were made.

### 5. Delete Document

- **Endpoint**: `/documents/<document_id>`
- **Method**: `DELETE`
- **Description**: Deletes a document by its ID.
- **Parameters**:
  - `document_id` (path): The ID of the document to delete.
- **Response**:
  - `200 OK`: Returns a success message if the document was deleted.
  - `404 Not Found`: If the document is not found.
