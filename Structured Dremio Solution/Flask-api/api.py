from flask import Flask, jsonify, request
import pandas as pd
import io
from dotenv import load_dotenv
import os
import requests
import re

# Load environment variables from .env file
load_dotenv('api.env')

app = Flask(__name__)

# Dremio configuration
dremio_url = os.getenv('DREMIO_URL')
dremio_username = os.getenv('DREMIO_USERNAME')
dremio_password = os.getenv('DREMIO_PASSWORD')

# Authenticate and get token
def get_dremio_token():
    auth_response = requests.post(f'{dremio_url}/apiv2/login', json={'userName': dremio_username, 'password': dremio_password})
    auth_response.raise_for_status()
    return auth_response.json().get('token')

# Function to execute SQL query on Dremio
def execute_dremio_query(sql):
    token = get_dremio_token()
    headers = {
        'Authorization': f'_dremio{token}',
        'Content-Type': 'application/json'
    }
    response = requests.post(f'{dremio_url}/api/v3/sql', headers=headers, json={'sql': sql})
    response.raise_for_status()
    job_id = response.json().get('id')
    return job_id

# Function to get query results from Dremio
def get_dremio_query_results(job_id):
    token = get_dremio_token()
    headers = {
        'Authorization': f'_dremio{token}',
        'Content-Type': 'application/json'
    }
    # Poll the job status endpoint until the job is complete
    while True:
        response = requests.get(f'{dremio_url}/api/v3/job/{job_id}', headers=headers)
        response.raise_for_status()
        job_status = response.json().get('jobState')
        if job_status == 'COMPLETED':
            break
        elif job_status in ('FAILED', 'CANCELED'):
            raise Exception(f'Query failed with status: {job_status}')
    
    # Fetch the query results
    response = requests.get(f'{dremio_url}/api/v3/job/{job_id}/results', headers=headers)
    response.raise_for_status()
    return response.json()

# Function to list catalog items from Dremio
def list_dremio_catalog():
    token = get_dremio_token()
    headers = {
        'Authorization': f'_dremio{token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(f'{dremio_url}/api/v3/catalog', headers=headers)
    response.raise_for_status()
    return response.json()

@app.route('/dremio_query', methods=['POST'])
def dremio_query():
    sql = request.json.get('sql')
    if not sql:
        return jsonify({'error': 'SQL query is required'}), 400
    
    # Validate that the query is a SELECT query and does not contain harmful commands
    harmful_commands = r'\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|EXEC|EXECUTE|GRANT|REVOKE|SET|USE|CALL|LOCK|UNLOCK|RENAME|COMMENT|COMMIT|ROLLBACK|SAVEPOINT|RELEASE)\b'
    if not re.match(r'^\s*SELECT\b', sql.strip(), re.IGNORECASE) or re.search(harmful_commands, sql, re.IGNORECASE):
        return jsonify({'error': 'Only SELECT queries are allowed and no harmful commands are permitted'}), 400
    
    try:
        job_id = execute_dremio_query(sql)
        result = get_dremio_query_results(job_id)
        return jsonify(result)
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/dremio_catalog', methods=['GET'])
def dremio_catalog():
    try:
        catalog = list_dremio_catalog()
        return jsonify(catalog)
    except requests.exceptions.RequestException as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('FLASK_RUN_PORT', 5000))
    app.run(host='0.0.0.0', port=port)