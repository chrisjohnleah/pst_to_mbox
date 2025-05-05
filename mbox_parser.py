import os
import mailbox
import logging
import email.utils
import sqlite3
from typing import List, Dict, Union, Optional

from db_manager import create_db, store_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_mbox_file(mbox_file: str, output_dir: str, db_connection: Optional[sqlite3.Connection] = None, source_pst: str = "") -> List[Dict[str, str]]:
    """Parse an MBOX file and extract email details.
    
    Args:
        mbox_file: Path to the MBOX file
        output_dir: Directory to save attachments
        db_connection: Optional SQLite connection to reuse
        source_pst: Source PST file name for tracking origin
        
    Returns:
        List of dictionaries containing email details
    """
    data = []
    save_dir = os.path.join(output_dir, 'attachments')
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    # Determine whether we need to close the connection later
    close_connection = False
    if db_connection is None:
        # Create a default database if none provided
        create_db()
        db_connection = sqlite3.connect('emaildb.sqlite3')
        close_connection = True
        
    try:
        # Start a transaction for better performance
        db_connection.execute('BEGIN TRANSACTION')
        
        mbox = mailbox.mbox(mbox_file)
        logging.info(f"Processing {len(mbox)} messages from {mbox_file}")
        
        for message in mbox:
            # Extract email details
            subject = message.get('subject', '')
            sender_info = message.get('from', '')
            date = message.get('date', '')
            receiver_info = message.get('to', '')
            
            if not (subject and sender_info and receiver_info):
                logging.warning("Skipping message with missing fields.")
                continue
                
            sender_name, sender_email = email.utils.parseaddr(sender_info)
            receiver_name, receiver_email = email.utils.parseaddr(receiver_info)
            
            # Check for attachments
            has_attachments = False
            for part in message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                    
                attachment_name = part.get_filename()
                if not attachment_name:
                    continue
                    
                has_attachments = True
                content_type = part.get_content_type()
                attachment_data = part.get_payload(decode=True)
                    
                if attachment_data:
                    attachment_path = os.path.join(save_dir, attachment_name)
                    try:
                        with open(attachment_path, 'wb') as file:
                            file.write(attachment_data)
                        logging.info(f"Saved attachment: {attachment_name}")
                    except IOError as e:
                        logging.error(f"Failed to save attachment {attachment_name}: {e}")
                        continue
                        
                    email_data = {
                        'subject': subject,
                        'sender_name': sender_name,
                        'sender_email': sender_email,
                        'recipient_name': receiver_name,
                        'recipient_email': receiver_email,
                        'attachment_filename': attachment_name,
                        'attachment_type': content_type,
                        'email_date': date,
                        'source_pst': source_pst
                    }
                    data.append(email_data)
                    store_data(email_data, db_connection.path)
                    
            # If no attachments, still store the email details
            if not has_attachments:
                email_data = {
                    'subject': subject,
                    'sender_name': sender_name,
                    'sender_email': sender_email,
                    'recipient_name': receiver_name,
                    'recipient_email': receiver_email,
                    'attachment_filename': '',
                    'attachment_type': '',
                    'email_date': date,
                    'source_pst': source_pst
                }
                data.append(email_data)
                store_data(email_data, db_connection.path)
        
        # Commit the transaction
        db_connection.commit()
        logging.info(f"Processed {len(data)} emails from {mbox_file}")
        
    except Exception as e:
        db_connection.rollback()
        logging.error(f"Failed to parse mbox file {mbox_file}: {e}")
        raise
    finally:
        if close_connection:
            db_connection.close()
            
    return data
