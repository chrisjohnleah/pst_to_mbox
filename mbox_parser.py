import mailbox
import email.utils
import os
import logging
import sqlite3
from db_manager import create_db, store_data

create_db()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_mbox_file(mbox_file, output_dir, db_connection=None):
    """
    Parse an MBOX file and extract email information.
    
    Args:
        mbox_file: Path to the MBOX file to parse
        output_dir: Directory to save attachments
        db_connection: Optional SQLite connection object for batch processing
    
    Returns:
        List of dictionaries containing email data
    """
    data = []
    save_dir = os.path.join(output_dir, 'attachments')
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    
    # Track if we should close the connection at the end
    close_connection = False
    if db_connection is None:
        db_connection = sqlite3.connect('emaildb.sqlite3')
        close_connection = True
    
    cursor = db_connection.cursor()

    try:
        mbox = mailbox.mbox(mbox_file)
        total_messages = len(mbox)
        processed = 0
        
        logging.info(f"Processing {total_messages} messages from {mbox_file}")
        
        for message in mbox:
            processed += 1
            if processed % 100 == 0:
                logging.info(f"Processed {processed}/{total_messages} messages")
                
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
            
            # Track attachments for this email
            attachments = []
            
            # Process all parts of the message
            for part in message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                    
                # Check if this part is an attachment
                if part.get('Content-Disposition') is None:
                    continue

                attachment_name = part.get_filename()
                if not attachment_name:
                    continue

                content_type = part.get_content_type()
                attachment_data = part.get_payload(decode=True)

                if attachment_data:
                    attachment_path = os.path.join(save_dir, attachment_name)
                    try:
                        with open(attachment_path, 'wb') as file:
                            file.write(attachment_data)
                        logging.info(f"Saved attachment: {attachment_name}")
                        
                        # Add to attachments list
                        attachments.append({
                            'name': attachment_name,
                            'content_type': content_type
                        })
                    except IOError as e:
                        logging.error(f"Failed to save attachment {attachment_name}: {e}")
            
            # Store each email exactly once
            if attachments:
                # If there are attachments, store one record per attachment
                for attachment in attachments:
                    email_data = {
                        'subject': subject,
                        'sender_name': sender_name,
                        'sender_email': sender_email,
                        'receiver_name': receiver_name,
                        'receiver_email': receiver_email,
                        'attachment_name': attachment['name'],
                        'content_type': attachment['content_type'],
                        'datetime': date,
                    }
                    data.append(email_data)
                    
                    # Store directly in database with the connection
                    try:
                        insert_sql = '''
                        INSERT INTO mytable (subject, sender_name, sender_email, receiver_name, 
                                            receiver_email, attachment_name, content_type, datetime)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        '''
                        cursor.execute(insert_sql, (
                            email_data['subject'],
                            email_data['sender_name'],
                            email_data['sender_email'],
                            email_data['receiver_name'],
                            email_data['receiver_email'],
                            email_data['attachment_name'],
                            email_data['content_type'],
                            email_data['datetime']
                        ))
                    except sqlite3.Error as e:
                        logging.error(f"Database error: {e}")
            else:
                # Store the email even if it has no attachments
                email_data = {
                    'subject': subject,
                    'sender_name': sender_name,
                    'sender_email': sender_email,
                    'receiver_name': receiver_name,
                    'receiver_email': receiver_email,
                    'attachment_name': '',  # No attachment
                    'content_type': '',
                    'datetime': date,
                }
                data.append(email_data)
                
                # Store directly in database with the connection
                try:
                    insert_sql = '''
                    INSERT INTO mytable (subject, sender_name, sender_email, receiver_name, 
                                        receiver_email, attachment_name, content_type, datetime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    '''
                    cursor.execute(insert_sql, (
                        email_data['subject'],
                        email_data['sender_name'],
                        email_data['sender_email'],
                        email_data['receiver_name'],
                        email_data['receiver_email'],
                        email_data['attachment_name'],
                        email_data['content_type'],
                        email_data['datetime']
                    ))
                except sqlite3.Error as e:
                    logging.error(f"Database error: {e}")
            
            # Commit every 100 messages for balance between performance and safety
            if processed % 100 == 0:
                db_connection.commit()
        
        # Final commit for any remaining transactions
        db_connection.commit()
        logging.info(f"Completed processing {processed} messages from {mbox_file}")
        
    except Exception as e:
        logging.error(f"Failed to parse mbox file {mbox_file}: {e}")
    finally:
        # Only close if we opened it ourselves
        if close_connection:
            db_connection.close()

    return data
