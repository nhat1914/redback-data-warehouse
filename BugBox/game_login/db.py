import sqlite3
import bcrypt

# Connect to SQLite database
conn = sqlite3.connect('school_kids.db')
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS students (
        student_id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        grade TEXT NOT NULL,
        class_letter TEXT NOT NULL,
        roll_number INTEGER NOT NULL,
        teacher_first_name TEXT NOT NULL,
        teacher_last_name TEXT NOT NULL,
        unique_id TEXT NOT NULL UNIQUE
     )          
''')

# Create the staff table 
cursor.execute('''
    CREATE TABLE IF NOT EXISTS staff (
        staff_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user'  -- Default role is 'user'
    )
''')

# Function to hash a password using bcrypt
def hash_password(password):
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Function to update the staff table with a hashed password
def insert_admin_user():
    # Hash the password before inserting it into the database
    hashed_password = hash_password('dylanBugbox')
    
    # Insert the admin user into the staff table
    cursor.execute('''INSERT INTO staff (username, password, role) VALUES (?, ?, ?)''', ('dylan', hashed_password, 'admin'))

# Check if the 'dylan' admin user already exists
cursor.execute('SELECT * FROM staff WHERE username = ?', ('dylan',))
admin_user = cursor.fetchone()

if not admin_user:
    # If 'dylan' admin doesn't exist, insert it
    insert_admin_user()

# Commit changes and close connection
conn.commit()
conn.close()
