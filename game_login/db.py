import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('school_kids.db')
cursor = conn.cursor()

# Create tables
cursor.execute('''
CREATE TABLE IF NOT EXISTS students (
        student_id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_name TEXT NOT NULL,
        grade TEXT NOT NULL,
        class_letter TEXT NOT NULL,
        roll_number INTEGER NOT NULL,
        teacher_name TEXT NOT NULL,
        unique_id TEXT NOT NULL UNIQUE
    )
''')

# Create the staff table 
cursor.execute('''
    CREATE TABLE IF NOT EXISTS staff (
        staff_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL,
        password TEXT NOT NULL
    )
''')

# Insert sample staff data
cursor.execute('''
INSERT INTO staff (username, password) VALUES (?, ?)
''', ('daezel', 'daezel123'))

# Commit changes and close connection
conn.commit()
conn.close()
