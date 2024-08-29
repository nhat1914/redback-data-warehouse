import requests
import pandas as pd
import sqlite3
import getpass
import time

# List of CSV file URLs from GitHub
csv_urls = [
    'https://raw.githubusercontent.com/Redback-Operations/redback-fit-sports-performance/main/Cycling%20Analysis/data/extended_activities.csv'
]

# CHANGE THESE FOR USE CASE
dremio_url = input('Enter the Dremio URL: ')
username = input('Enter the Dremio username: ')
password = getpass.getpass(prompt='Enter your Dremio password: ')
source = input('Enter the Dremio source: ')
chunk_size = 50 * 1024 * 1024  # 50MB chunk size (change as needed)

# Authenticate and get token
auth_response = requests.post(f'{dremio_url}/apiv2/login', json={'userName': username, 'password': password})
auth_token = auth_response.json().get('token')

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
    sql_response = requests.post(f'{dremio_url}/api/v3/sql', headers=headers, json={'sql': command})
    if sql_response.status_code != 200:
        print(f'Failed to execute SQL command: {sql_response.status_code}')
        print(f'Response content: {sql_response.content}')
    else:
        print(f'Executed SQL command: {sql_response.status_code}')
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

# Download CSV files, convert to SQL, and upload to Dremio
for url in csv_urls:
    response = requests.get(url)
    file_name = url.split('/')[-1]
    table_name = file_name.split('.')[0]
    
    # Read CSV in chunks
    for chunk in pd.read_csv(url, chunksize=1000):  # Adjust the chunksize as needed for performance
        # Convert chunk to SQL table using in-memory SQLite database
        conn = sqlite3.connect(':memory:')
        chunk.to_sql(table_name, conn, if_exists='append', index=False)

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
