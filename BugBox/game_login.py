import streamlit as st
import qrcode
from PIL import Image
from io import BytesIO
import cv2
import numpy as np
import pyzbar.pyzbar as pyzbar
import sqlite3
import pandas as pd

# Display the title at the top
st.markdown("<h1 style='text-align: left; color: black;'>Bugbox</h1>", unsafe_allow_html=True)

# Ensure session state is initialized
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# Function to generate a unique ID
def generate_unique_id(name, grade, class_letter, roll_number):
    return f"{grade}{class_letter}{name}{roll_number}"

# Function to generate QR code
def generate_qr_code(data):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill='black', back_color='white')
    return img

# Function to scan QR code from image
def scan_qr_code(image):
    decoded_objects = pyzbar.decode(image)
    for obj in decoded_objects:
        return obj.data.decode('utf-8')
    return None

# Function to retrieve student name from the database using unique ID
def get_student_name_from_db(unique_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT student_name FROM students WHERE unique_id=?", (unique_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0]  # Return the student's name
    else:
        return None

# Function to open webcam and scan QR code
def webcam_qr_scanner():
    cap = cv2.VideoCapture(0)
    st.text("Opening webcam...")
    st.text("Please place your QR Code in front of the webcam!!")

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            st.text("Failed to capture image.")
            break
        frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        qr_data = scan_qr_code(frame_rgb)
        if qr_data:
            st.success(f"QR Code Data: {qr_data}")
            cap.release()
            cv2.destroyAllWindows()

            # Retrieve the student's name from the database using the unique ID
            student_name = get_student_name_from_db(qr_data)
            
            if student_name:
                # Display the greeting message
                st.success(f"Hi {student_name}! 🎉")
                st.markdown("<h3>🎮 Get ready for some fun and adventure! 🚀</h3>", unsafe_allow_html=True)
            else:
                st.error("Student not found in the database.")

            return qr_data
        cv2.imshow('Webcam QR Code Scanner', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    cap.release()
    cv2.destroyAllWindows()
    return None

# Function to connect to the database
def get_db_connection():
    conn = sqlite3.connect('school_kids.db')
    return conn

# Function to store student details in the database
def store_student_in_db(name, grade, class_letter, roll_number, teacher_name, unique_id):
    conn = get_db_connection()
    c = conn.cursor()
    # Check for duplicate roll number in the same grade and class
    c.execute('''
        SELECT * FROM students WHERE grade=? AND class_letter=? AND roll_number=?
    ''', (grade, class_letter, roll_number))
    result = c.fetchone()

    if result:
        # If a record is found, return an error
        conn.close()
        return False
    else:
        # If no duplicate is found, insert the new record
        c.execute('''
            INSERT INTO students (student_name, grade, class_letter, roll_number, teacher_name, unique_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, grade, class_letter, roll_number, teacher_name, unique_id))
        conn.commit()
        conn.close()
        return True

# Function to export data to CSV
def export_data_to_csv():
    conn = get_db_connection()
    query = "SELECT * FROM students"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert the dataframe to CSV
    csv_data = df.to_csv(index=False)
    
    # Generate a download link
    st.download_button(
        label="Download Student Data as CSV",
        data=csv_data,
        file_name='student_data.csv',
        mime='text/csv',
    )    

# Tabs for different functionalities
tabs = st.tabs(["Login", "Generate QR Code", "Staff Login", "Recover QR Code"])

# Tab 1: QR Code Login
with tabs[0]:
    st.title("Login using QR Code")
    if st.button("Scan QR Code with Webcam", key="scan_button_login"):
        qr_data = webcam_qr_scanner()
        if qr_data:
            st.success(f"Logged in successfully with ID: {qr_data}")
        else:
            st.error("Failed to read QR code. Please try again.")

# Tab 2: Unique ID & QR Code Generation
with tabs[1]:
    st.title("Generate QR Code")
    name = st.text_input("Enter the student's name:", key="name_generate")
    grade = st.text_input("Enter the student's grade:", key="grade_generate")
    class_letter = st.text_input("Enter the student's class letter:", key="class_letter_generate")
    roll_number = st.number_input("Enter the student's roll number:", min_value=1, key="roll_number_generate")
    teacher_name = st.text_input("Enter the class teacher's name:", key="teacher_name_generate")

    if st.button("Generate QR Code", key="generate_qr_button"):
        if name and grade and class_letter and roll_number and teacher_name:
            unique_id = generate_unique_id(name, grade, class_letter, roll_number)

            success = store_student_in_db(name, grade, class_letter, roll_number, teacher_name, unique_id)

            if success:
                # Generate QR code only if student data is successfully stored
                img = generate_qr_code(unique_id)
                buf = BytesIO()
                img.save(buf, format="PNG")
                buf.seek(0)
                img = Image.open(buf)
                st.image(img, caption='Scan this QR code to log in', use_column_width=True)
                st.success("Student details stored successfully!")
                
                # Provide download link for the QR code
                st.download_button(
                    label="Download QR Code",
                    data=buf,
                    file_name=f"{unique_id}_QRCode.png",
                    mime="image/png"
                )
            else:
                st.error(f"A student with roll number {roll_number} already exists in grade {grade}, class {class_letter}. Please check your details!")
        else:
            st.error("Please fill in all the fields.")

# Tab 3: Staff Login
with tabs[2]:
    st.title("Staff Login")

    if not st.session_state['logged_in']:
        with st.form(key='staff_login_form'):
            username = st.text_input("Username", key="staff_username")
            password = st.text_input("Password", type="password", key="staff_password")
            login_button = st.form_submit_button("Login")

        if login_button:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM staff WHERE username=? AND password=?", (username, password))
            staff = cursor.fetchone()
            
            if staff:
                st.session_state['logged_in'] = True
                st.success("Login successful!")
            else:
                st.error("Invalid login details")
    else:
        st.write("Login successful!")
        if st.button("Logout", key="logout_button"):
            st.session_state['logged_in'] = False

    # Display the student database if logged in
    if st.session_state.get('logged_in'):
        st.title("Student Database")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM students")
        students = cursor.fetchall()
        for student in students:
            st.write(student)
        
        # Add an export button
        export_data_to_csv()

# Tab 4: Recover QR Code
with tabs[3]:
    st.title("Recover QR Code")
    st.write("Enter the student details to recover the QR Code.")

    name = st.text_input("Enter the student's name:", key="name_recover")
    grade = st.text_input("Enter the student's grade:", key="grade_recover")
    class_letter = st.text_input("Enter the student's class letter:", key="class_letter_recover")
    roll_number = st.number_input("Enter the student's roll number:", min_value=1, key="roll_number_recover")

    if st.button("Recover QR Code", key="recover_qr_button"):
        if name and grade and class_letter and roll_number:
            unique_id = generate_unique_id(name, grade, class_letter, roll_number)
            img = generate_qr_code(unique_id)
            buf = BytesIO()
            img.save(buf, format="PNG")
            buf.seek(0)
            img = Image.open(buf)
            st.image(img, caption='Scan this QR code to log in', use_column_width=True)

            # Provide download link for the regenerated QR code
            st.download_button(
                label="Download QR Code",
                data=buf,
                file_name=f"{unique_id}_QRCode.png",
                mime="image/png"
            )
        else:
            st.error("Please fill in all the fields.")
