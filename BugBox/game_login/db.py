import sqlite3
import os
import bcrypt
from dotenv import load_dotenv
load_dotenv()
hashed_password = bcrypt.hashpw(os.getenv('ADMIN_PASSWORD').encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Connect to SQLite database
conn = sqlite3.connect('school_kids.db')
cursor = conn.cursor()

# Create tables if they don't exist
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

cursor.execute('''
    CREATE TABLE IF NOT EXISTS staff (
        staff_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        password TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'user'  -- Default role is 'user'
    )
''')

# Check if admin user already exists
cursor.execute('SELECT COUNT(*) FROM staff WHERE username = ?', ('dylan',))
admin_exists = cursor.fetchone()[0]

# Insert the pre-hashed password for the admin user if not exists
if admin_exists == 0:
    cursor.execute('''INSERT INTO staff (username, password, role) VALUES (?, ?, ?)''', ('dylan', hashed_password, 'admin'))
    print("Admin user created.")
else:
    print("Admin user already exists.")

conn.commit()
conn.close()