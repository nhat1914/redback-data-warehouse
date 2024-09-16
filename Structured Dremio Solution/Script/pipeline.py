# Ensure you have env file containing details for dremio in your working directory before running this script
# When running the script using: python pipeline.py
# Add a space then a url for the csv files being uploaded to dremio. Each subsequent url should be separated by a space.

import requests
import pandas as pd
import sqlite3
import getpass
import time
from dotenv import load_dotenv
import os
import argparse
import logging
import sys
from urllib.parse import urlparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to get environment variables with error handling
def get_env_variable(var_name):
    value = os.getenv(var_name)
    if not value:
        logging.error(f"Environment variable {var_name} is not set.")
        sys.exit(1)
    return value

# Function to validate URLs
def is_valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

# Load environment variables from .env file
load_dotenv('pipeline.env')

# Set up argument parsing
parser = argparse.ArgumentParser(description='Process CSV URLs.')
parser.add_argument('csv_urls', nargs='+', help='List of CSV file URLs')
args = parser.parse_args()

# Get CSV URLs from command line arguments
csv_urls = args.csv_urls

# Validate CSV URLs
validated_urls = [url for url in csv_urls if is_valid_url(url)]
if len(validated_urls) != len(csv_urls):
    logging.error("One or more provided URLs are invalid.")
    sys.exit(1)

# Get environment variables with error handling
dremio_url = get_env_variable('DREMIO_URL')
username = get_env_variable('DREMIO_USERNAME')
password = get_env_variable('DREMIO_PASSWORD')
source = get_env_variable('DREMIO_SOURCE')

chunk_size = 50 * 1024 * 1024  # 50MB chunk size (change as needed)

# Authenticate and get token
try:
    auth_response = requests.post(f'{dremio_url}/apiv2/login', json={'userName': username, 'password': password})
    auth_response.raise_for_status()
    auth_token = auth_response.json().get('token')
except requests.exceptions.RequestException as e:
    logging.error(f"Error occurred while making a request: {str(e)}")
    sys.exit(1)

# Headers for authenticated requests
headers = {
    'Authorization': f'_dremio{auth_token}',
    'Content-Type': 'application/json'
}

# Function to filter out unsupported SQL commands
def filter_sql_commands(commands):
    supported_commands = [
        "CREATE", "INSERT", "DELETE", "UPDATE", "DROP", "ALTER", "TRUNCATE", "SELECT", "VALUES"
    ]
    filtered_commands = []
    for command in commands:
        if any(command.strip().upper().startswith(cmd) for cmd in supported_commands):
            filtered_commands.append(command)
    return filtered_commands

# Mapping of SQLite types to Dremio types
type_mapping = {
    "INTEGER": "INT",
    "TEXT": "VARCHAR",
    "REAL": "FLOAT",
    "BLOB": "VARBINARY",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "FLOAT": "FLOAT",
    "DECIMAL": "DECIMAL",
    "DOUBLE": "DOUBLE",
    "INTERVAL": "INTERVAL",
    "BIGINT": "BIGINT",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP"
}

def convert_sqlite_to_dremio(sql_commands):
    converted_commands = []
    for command in sql_commands:
        for sqlite_type, dremio_type in type_mapping.items():
            command = command.replace(sqlite_type, dremio_type)
        converted_commands.append(command)
    return converted_commands

# Function to send SQL command to Dremio
def send_sql_command(command):
    try:
        sql_response = requests.post(f'{dremio_url}/api/v3/sql', headers=headers, json={'sql': command})
        sql_response.raise_for_status()
        logging.info(f'Executed SQL command: {sql_response.status_code}')
    except requests.exceptions.RequestException as e:
        logging.error(f'Failed to execute SQL command: {str(e)}')
        logging.error(f'Response content: {sql_response.content}')
        sys.exit(1)
    time.sleep(5)

# Function to combine INSERT commands into a single statement
def combine_insert_commands(insert_commands):
    if not insert_commands:
        return None
    
    # Extract the base part of the INSERT statement (i.e., columns part)
    base_insert = insert_commands[0].split(' VALUES', 1)[0]  # Get the INSERT part

    # Combine all VALUES parts into a single statement
    combined_values = []
    for command in insert_commands:
        values_part = command.split(' VALUES', 1)[1].strip().rstrip(';')
        combined_values.append(values_part)

    combined_insert = f'{base_insert} VALUES {", ".join(combined_values)};'
    return combined_insert

# Function to send accumulated SQL commands in chunks
def send_sql_in_chunks(filtered_commands, max_chunk_size):
    chunk = []
    chunk_size = 0
    for command in filtered_commands:
        command_size = len(command.encode('utf-8'))
        if chunk_size + command_size > max_chunk_size:
            # Combine INSERT commands in the current chunk
            combined_insert_command = combine_insert_commands(chunk)
            if combined_insert_command:
                send_sql_command(combined_insert_command)
            # Reset chunk and chunk size
            chunk = []
            chunk_size = 0
        
        chunk.append(command)
        chunk_size += command_size

    # Send any remaining commands in the last chunk
    if chunk:
        combined_insert_command = combine_insert_commands(chunk)
        if combined_insert_command:
            send_sql_command(combined_insert_command)

# Function to insert data using prepared statements
def insert_data(conn, table_name, data):
    placeholders = ', '.join(['?' for _ in data[0]])
    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor = conn.cursor()
    try:
        cursor.executemany(query, data)
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database error occurred: {str(e)}")
        sys.exit(1)

# Download CSV files, convert to SQL, and upload to Dremio
logging.info("Script started")
logging.info(f"Processing {len(validated_urls)} CSV files")
for url in validated_urls:
    try:
        response = requests.get(url)
        response.raise_for_status()
        file_name = url.split('/')[-1]
        table_name = file_name.split('.')[0]
        
        # Read CSV in chunks
        for chunk in pd.read_csv(url, chunksize=1000):  # Adjust the chunksize as needed for performance
            # Convert chunk to SQL table using in-memory SQLite database
            conn = sqlite3.connect(':memory:')
            
            # Create table schema based on the first chunk
            chunk.to_sql(table_name, conn, if_exists='replace', index=False)
            
            # Insert data into the table
            insert_data(conn, table_name, chunk.values.tolist())

            # Extract SQL commands to create and populate the table
            sql_commands = []
            for line in conn.iterdump():
                sql_commands.append(line)
            conn.close()

            # Convert SQLite types to Dremio types
            sql_commands = convert_sqlite_to_dremio(sql_commands)

            # Filter out unsupported SQL commands
            filtered_commands = filter_sql_commands(sql_commands)

            # Separate CREATE TABLE command from INSERT commands
            create_table_command = None
            insert_commands = []
            for command in filtered_commands:
                if command.strip().upper().startswith("CREATE TABLE"):
                    create_table_command = command
                else:
                    insert_commands.append(command)

            # Modify the table name to include the full path
            full_table_path = f'"{source}"."{table_name}"'
            if create_table_command:
                create_table_command = create_table_command.replace(f'"{table_name}"', full_table_path)
                # Send the CREATE TABLE command
                send_sql_command(create_table_command)

            insert_commands = [cmd.replace(f'"{table_name}"', full_table_path) for cmd in insert_commands]

            # Upload INSERT commands in chunks to Dremio
            send_sql_in_chunks(insert_commands, chunk_size)  # 50MB chunk size
    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred while downloading CSV file: {str(e)}")
        sys.exit(1)
    except pd.errors.ParserError as e:
        logging.error(f"Error occurred while parsing CSV file: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)
logging.info("Script completed")