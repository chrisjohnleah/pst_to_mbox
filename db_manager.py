import sqlite3
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_db(db_name: str = 'emaildb.sqlite3') -> bool:
    """Create the database and table if they do not exist."""
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            create_table_sql = '''
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY,
                subject TEXT,
                sender_name TEXT,
                sender_email TEXT,
                recipient_name TEXT,
                recipient_email TEXT,
                attachment_filename TEXT,
                attachment_type TEXT,
                email_date TEXT,
                source_pst TEXT
            )'''
            cursor.execute(create_table_sql)
            logging.info(f"Database and email table created successfully: {db_name}")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error creating database {db_name}: {e}")
        return False

def store_data(data_dict: Dict[str, str], db_name: str = 'emaildb.sqlite3') -> bool:
    """Store email data in the database."""
    required_keys = ['subject', 'sender_name', 'sender_email', 'recipient_name', 'recipient_email', 'attachment_filename', 'attachment_type', 'email_date']
    
    if not all(key in data_dict for key in required_keys):
        logging.error("Data dictionary is missing required keys.")
        return False

    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        data_to_insert = (
            data_dict['subject'],
            data_dict['sender_name'],
            data_dict['sender_email'],
            data_dict['recipient_name'],
            data_dict['recipient_email'],
            data_dict['attachment_filename'],
            data_dict['attachment_type'], 
            data_dict['email_date'],
            data_dict.get('source_pst', '')  # Make source_pst optional with default empty string
        )

        # Insert data into the table
        insert_sql = '''
        INSERT INTO emails (subject, sender_name, sender_email, recipient_name, recipient_email, attachment_filename, attachment_type, email_date, source_pst)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(insert_sql, data_to_insert)

        # Commit the changes and close the database connection
        conn.commit()
        conn.close()
        logging.info("Email data inserted in database successfully.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error storing data: {e}")
        return False
