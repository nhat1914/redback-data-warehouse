import sqlite3

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

# Insert the pre-hashed password for the admin user
hashed_password = "$2b$12$wQ15tkMWRN69.1Lr0qO4KOSl1dkc7GxvsVKQJ9T3AluaMJjTd4Xku"
cursor.execute('''INSERT INTO staff (username, password, role) VALUES (?, ?, ?)''', ('dylan', hashed_password, 'admin'))

# Commit changes and close connection
conn.commit()
conn.close()
