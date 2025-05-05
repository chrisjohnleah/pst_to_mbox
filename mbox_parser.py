import mailbox
import email.utils
import os
import logging
from db_manager import create_db, store_data

create_db()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_mbox_file(mbox_file, output_dir):
    data = []
    save_dir = os.path.join(output_dir, 'attachments')
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    try:
        mbox = mailbox.mbox(mbox_file)
        for message in mbox:
            # Extract email details
            subject = message['subject']
            sender_info = message['from']
            date = message['date']
            receiver_info = message['to']

            if not (subject and sender_info and receiver_info):
                logging.warning("Skipping message with missing fields.")
                continue

            sender_name, sender_email = email.utils.parseaddr(sender_info)
            receiver_name, receiver_email = email.utils.parseaddr(receiver_info)

            for part in message.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
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
                    except IOError as e:
                        logging.error(f"Failed to save attachment {attachment_name}: {e}")
                        continue

                email_data = {
                    'subject': subject,
                    'sender_name': sender_name,
                    'sender_email': sender_email,
                    'receiver_name': receiver_name,
                    'receiver_email': receiver_email,
                    'attachment_name': attachment_name,
                    'content_type': content_type,
                    'datetime': date,
                }
                data.append(email_data)
                if not store_data(email_data):
                    logging.error(f"Failed to store data for email: {subject}")
    except Exception as e:
        logging.error(f"Failed to parse mbox file {mbox_file}: {e}")

    return data



