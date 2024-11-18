# Test file with intentional security vulnerabilities

# SQL Injection vulnerability
def unsafe_sql_query(user_input):
    query = f"SELECT * FROM users WHERE username = '{user_input}'"
    return query

# Hardcoded secrets
API_KEY = "1234567890abcdef"
SECRET_TOKEN = "my_super_secret_token"

# Command injection vulnerability
def unsafe_command(user_input):
    import os
    os.system(f"echo {user_input}")

# Unsafe deserialization
def unsafe_pickle(data):
    import pickle
    return pickle.loads(data)

# Path traversal
def unsafe_file_access(filename):
    with open(f"../{filename}", "r") as f:
        return f.read()

# SSRF vulnerability
def unsafe_request(url):
    import requests
    response = requests.get(url + "/admin")
    return response.text