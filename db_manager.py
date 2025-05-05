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
            CREATE TABLE IF NOT EXISTS mytable (
                id INTEGER PRIMARY KEY,
                subject TEXT,
                sender_name TEXT,
                sender_email TEXT,
                receiver_name TEXT,
                receiver_email TEXT,
                attachment_name TEXT,
                content_type TEXT,
                datetime TEXT
            )'''
            cursor.execute(create_table_sql)
            logging.info("Database and table created successfully.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error creating database: {e}")
        return False

def store_data(data_dict: Dict[str, str], db_name: str = 'emaildb.sqlite3') -> bool:
    """Store data in the database."""
    required_keys = ['subject', 'sender_name', 'sender_email', 'receiver_name', 'receiver_email', 'attachment_name', 'content_type', 'datetime']
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
            data_dict['receiver_name'],
            data_dict['receiver_email'],
            data_dict['attachment_name'],
            data_dict['content_type'], 
            data_dict['datetime']
        )

        # Insert data into the table
        insert_sql = '''
        INSERT INTO mytable (subject, sender_name, sender_email, receiver_name, receiver_email, attachment_name, content_type, datetime)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(insert_sql, data_to_insert)

        # Commit the changes and close the database connection
        conn.commit()
        conn.close()
        logging.info("Data inserted in db successfully.")
        return True
    except sqlite3.Error as e:
        logging.error(f"Error storing data: {e}")
        return False
