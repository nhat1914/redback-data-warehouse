#First, run 
# pip install -r requirements.txt 
# in terminal

import streamlit as st
import qrcode
from PIL import Image
from io import BytesIO
import cv2
import numpy as np
import pyzbar.pyzbar as pyzbar
import zipfile
import sqlite3
import bcrypt
import pandas as pd
from PIL import ImageDraw, ImageFont

# Display the title at the top
st.markdown("<h1 style='text-align: left; color: black;'>Bugbox</h1>", unsafe_allow_html=True)

# Ensure session state is initialized
if not st.session_state.get('logged_in'):
    st.session_state['logged_in'] = False

# Function to generate a unique ID
def generate_unique_id(first_name, last_name, grade, class_letter, roll_number):
    return f"{grade}{class_letter}{first_name}{last_name}{roll_number}"

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
    cursor.execute("SELECT first_name, last_name FROM students WHERE unique_id=?", (unique_id,))
    result = cursor.fetchone()
    conn.close()
    if result:
        return f"{result[0]} {result[1]}"  # Return the student's full name
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
def store_student_in_db(first_name, last_name, grade, class_letter, roll_number, teacher_first_name, teacher_last_name, unique_id):
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
            INSERT INTO students (first_name, last_name, grade, class_letter, roll_number, teacher_first_name, teacher_last_name, unique_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (first_name, last_name, grade, class_letter, roll_number, teacher_first_name, teacher_last_name, unique_id))
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

# Function to download all QR codes as PNGs
def download_all_qr_codes():
    
    # Ensure the user is an admin before proceeding
    if st.session_state.get('role') != 'admin':
        st.error("Unauthorized access. Only admin users can download QR codes.")
        return None
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT unique_id FROM students")
        students = cursor.fetchall()
        conn.close()

        zip_buffer = BytesIO()
        font = ImageFont.load_default()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            for student in students:
                unique_id = student[0]
                
             # Sanitize the unique_id to prevent potential security issues
                sanitized_unique_id = "".join(c for c in unique_id if c.isalnum() or c in "-_")   
                
                img = generate_qr_code(sanitized_unique_id)
            
                # Create a new image with enough space for both the QR code and the caption
                img_width, img_height = img.size
                caption_height = 30  # Space for the caption
                
                # Create a new blank image (white background) with space for the caption
                img_with_caption = Image.new('RGB', (img_width, img_height + caption_height), 'white')
                
                # Paste the QR code onto the blank image
                img_with_caption.paste(img, (0, 0))
                
                # Create a drawing object to add the caption
                draw = ImageDraw.Draw(img_with_caption)
                
                # Use textbbox to calculate text size and center the caption
                bbox = draw.textbbox((0, 0), unique_id, font=font)
                text_width = bbox[2] - bbox[0]
                
                # Draw the caption centered below the QR code
                draw.text(((img_width - text_width) / 2, img_height + 5), sanitized_unique_id, font=font, fill='black')
                
                # Save the image to a buffer
                buf = BytesIO()
                img_with_caption.save(buf, format="PNG")
                buf.seek(0)
                
                # Add the image to the zip file
                zf.writestr(f"{sanitized_unique_id}_QRCode.png", buf.read())

        zip_buffer.seek(0)
        return zip_buffer

    except Exception as e:
        st.error(f"An error occurred while generating QR codes: {e}")
    finally:
        conn.close()

def check_password(stored_password, provided_password):
    # Checking if the provided password matches the stored hashed password
    return bcrypt.checkpw(provided_password.encode('utf-8'), stored_password.encode('utf-8'))

# Function to update staff password
def update_staff_password(username, new_password):
    # Hash the new password before updating it in the database
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE staff SET password=? WHERE username=?", (hashed_password, username))
    conn.commit()
    conn.close()

# Function to view the staff members
def view_staff_members():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT username , role FROM staff", conn)
    conn.close()
    return df

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
    first_name = st.text_input("Enter the student's first name:", key="first_name_generate")
    last_name = st.text_input("Enter the student's last name:", key="last_name_generate")
    grade = st.text_input("Enter the student's grade:", key="grade_generate")
    class_letter = st.text_input("Enter the student's class letter:", key="class_letter_generate")
    roll_number = st.number_input("Enter the student's roll number:", min_value=1, key="roll_number_generate")
    teacher_first_name = st.text_input("Enter the class teacher's first name:", key="teacher_first_name_generate")
    teacher_last_name = st.text_input("Enter the class teacher's last name:", key="teacher_last_name_generate")

    if st.button("Generate QR Code", key="generate_qr_button"):
        if first_name and last_name and grade and class_letter and roll_number and teacher_first_name and teacher_last_name:
            unique_id = generate_unique_id(first_name, last_name, grade, class_letter, roll_number)

            success = store_student_in_db(first_name, last_name, grade, class_letter, roll_number, teacher_first_name, teacher_last_name, unique_id)

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

    # Check if the user is logged in
    if not st.session_state['logged_in']:
        # Only display login form if the user is not logged in
        with st.form(key='staff_login_form'):
            username = st.text_input("Username", key="username_staff_login")
            password = st.text_input("Password", type="password", key="password_staff_login")
            login_button = st.form_submit_button("Login")

            if login_button:
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM staff WHERE username=? ", (username, ))
                staff = cursor.fetchone()

                if staff:
                    stored_password = staff[2]  
                    if check_password(stored_password, password):
                        st.session_state['logged_in'] = True
                        st.session_state['username'] = staff[1]
                        st.session_state['role'] = staff[3]  # Store the role
                        st.success(f"Login successful! Welcome, {staff[1]}")
                    else:
                        st.error("Invalid login details")
                else:
                    st.error("Invalid login details")

    # If logged in, hide login form and show admin functionalities
    if st.session_state.get('logged_in', False):
        st.write(f"Logged in as {st.session_state['username']} with role: {st.session_state['role']}")

        # Display admin-only block
        if st.session_state['role'] == 'admin':
            st.success("You have admin access.")

            # 1. Expandable section for Signing Up New Staff Members
            with st.expander("Sign up New Staff Member"):
                st.subheader("Sign up New Staff Member")
                new_username = st.text_input("New staff username", key="new_username")
                new_password = st.text_input("New staff password", type="password", key="new_password")
                new_role = st.selectbox("Role", ['user', 'admin'], key="new_role")

                if st.button("Sign up new staff", key="sign_up_staff"):
                    if new_username and new_password:
                        # Hash the new staff password before saving to database
                        hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                        
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT INTO staff (username, password, role) VALUES (?, ?, ?)
                        ''', (new_username, hashed_password, new_role))
                        conn.commit()
                        conn.close()
                        st.success(f"New staff member '{new_username}' added successfully!")
                    else:
                        st.error("Please provide both username and password.")

            # 2. Expandable section for Removing Staff Members
            with st.expander("Remove a Staff Member"):
                st.subheader("Remove a Staff Member")

                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT username FROM staff WHERE username != ?", (st.session_state['username'],))
                staff_members = cursor.fetchall()
                conn.close()

                if staff_members:
                    staff_list = [staff[0] for staff in staff_members]  # Extract usernames

                    # Select staff member to remove
                    staff_to_remove = st.selectbox("Select a staff member to remove:", staff_list, key="staff_to_remove")

                    if st.button("Remove Staff", key="remove_staff"):
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        cursor.execute("DELETE FROM staff WHERE username=?", (staff_to_remove,))
                        conn.commit()
                        conn.close()
                        st.success(f"Staff member '{staff_to_remove}' removed successfully!")
                else:
                    st.info("No other staff members to remove.")

            # 3. Change Own Password
            with st.expander("Change Your Password"):
                st.subheader("Change Your Own Password")
                new_password = st.text_input("New password", type="password", key="new_password_own")
                if st.button("Change Own Password", key="change_own_password"):
                    if new_password:
                        update_staff_password(st.session_state['username'], new_password)
                        st.success("Password updated successfully!")

            # 4. Change Other Staff Password (Admin-only)
            with st.expander("Change Other Staff Password"):
                st.subheader("Change Another Staff Member's Password")
                staff_to_update = st.text_input("Enter staff username", key="staff_to_update")
                new_staff_password = st.text_input("New password", type="password", key="new_staff_password")
                if st.button("Change Staff Password", key="change_staff_password"):
                    if staff_to_update and new_staff_password:
                        update_staff_password(staff_to_update, new_staff_password)
                        st.success(f"Password for '{staff_to_update}' updated successfully!")

            #5. View staff members section
            with st.expander("View staff Member"):
                st.subheader("View Staff Members")
                conn = get_db_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT username, role FROM staff")
                staff_members = cursor.fetchall()
                conn.close()

                if staff_members:
                    staff_df = pd.DataFrame(staff_members, columns=["Username", "Role"])
                    st.dataframe(staff_df)
                else:
                    st.info("No staff members to show.")    

            #6. Add a button to download all QR codes
            with st.expander("Download QR Code Zip File"):
                st.subheader("Download All QR Codes Zip File")
                zip_buffer = download_all_qr_codes()
                st.download_button(
                    label="Download All QR Codes",
                    data=zip_buffer,
                    file_name='all_qr_codes.zip',
                    mime='application/zip'
                )
                
        else:
            st.info("You are logged in as a regular staff member.")

        # Logout button
        if st.button("Logout", key="logout_button"):
        # if st.session_state['logged_in']:
            # Reset session state on logout
            st.session_state['logged_in'] = False
            st.session_state['username'] = None
            st.session_state['role'] = None
            # Add a JavaScript snippet to refresh the page
            st.markdown("<meta http-equiv='refresh' content='0'>", unsafe_allow_html=True)

        # Display the student database if logged in
        st.title("Student Database")
        if st.button("View Student Database", key="view_student_database"):
            conn = get_db_connection()
            df = pd.read_sql_query("SELECT * FROM students", conn)
            st.dataframe(df)  # Display student data as a table
            conn.close()
        
        # Add an export button
        export_data_to_csv()

# Tab 4: Recover QR Code
with tabs[3]:
    st.title("Recover QR Code")
    st.write("Enter the student details to recover the QR Code.")

    first_name = st.text_input("Enter the student's first name:", key="first_name_recover")
    last_name = st.text_input("Enter the student's last name:", key="last_name_recover")
    grade = st.text_input("Enter the student's grade:", key="grade_recover")
    class_letter = st.text_input("Enter the student's class letter:", key="class_letter_recover")
    roll_number = st.number_input("Enter the student's roll number:", min_value=1, key="roll_number_recover")

    if st.button("Recover QR Code", key="recover_qr_button"):
        if first_name and last_name and grade and class_letter and roll_number:
            unique_id = generate_unique_id(first_name, last_name, grade, class_letter, roll_number)
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
