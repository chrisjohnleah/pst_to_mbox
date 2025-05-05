import os
import mailbox
import logging
import email.utils
import sqlite3
import re
import hashlib
from typing import List, Dict, Union, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path

from db_manager import create_db, store_data

# Configure logging - standard format without redaction
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Security configuration
SENSITIVE_KEYWORDS = [
    'password', 'secret', 'confidential', 'private', 'sensitive',
    'ssn', 'social security', 'credit card', 'account number', 'banking',
    'personal', 'restricted', 'classified', 'financial', 'medical'
]

class SecurityConfig:
    """Security configuration for email processing."""
    
    # Set to True to enable PII (Personally Identifiable Information) detection in logs only
    detect_pii = True
    
    # Set to True to sanitize attachment filenames to prevent path traversal attacks
    sanitize_filenames = True
    
    # Set to False to preserve original attachment filenames (recommended for migration)
    secure_filenames = False
    
    # Set to True to scan attachments for potentially malicious content (warning only)
    scan_attachments = True
    
    # Define potentially problematic attachment types (for warning only)
    potentially_dangerous_types = [
        'application/x-msdownload', 'application/x-executable',
        'application/x-dosexec', 'application/x-msdos-program',
        'application/bat', 'application/x-bat', 'application/x-sh'
    ]
    
    # Maximum attachment size in bytes (50 MB) - for warning only
    max_attachment_size = 50 * 1024 * 1024

def check_sensitive_content(content: str) -> bool:
    """Check if content contains potentially sensitive information (for logging warnings only).
    
    Args:
        content: Text content to check
        
    Returns:
        True if sensitive information is detected, False otherwise
    """
    if not content:
        return False
        
    # Convert to lowercase for case-insensitive matching
    content_lower = content.lower()
    
    # Check for sensitive keywords
    for keyword in SENSITIVE_KEYWORDS:
        if keyword in content_lower:
            return True
            
    # Check for common PII patterns if enabled
    if SecurityConfig.detect_pii:
        # Check for SSN (XXX-XX-XXXX)
        if re.search(r'\b\d{3}-\d{2}-\d{4}\b', content):
            return True
            
        # Check for credit card numbers
        if re.search(r'\b(?:\d{4}[- ]?){3}\d{4}\b', content):
            return True
            
    return False

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal attacks only.
    Do not change the actual name for migration purposes.
    
    Args:
        filename: Original filename
        
    Returns:
        Sanitized filename (for path safety only)
    """
    if not SecurityConfig.sanitize_filenames:
        return filename
        
    # Remove path components to prevent directory traversal
    filename = os.path.basename(filename)
    
    # Replace potentially dangerous characters
    filename = re.sub(r'[\\/:*?"<>|]', '_', filename)
    
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        filename = name[:255-len(ext)] + ext
        
    # Note: we're not generating random filenames for migration purposes
    # to preserve the original attachment names
        
    return filename

def check_attachment(content_type: str, attachment_data: bytes) -> bool:
    """Check attachment for potentially problematic characteristics.
    Logs warnings only, does not block or modify data.
    
    Args:
        content_type: MIME type of the attachment
        attachment_data: Attachment data
        
    Returns:
        True if attachment passes checks, False if warnings were generated
    """
    warnings_generated = False
    
    # Check attachment size
    if len(attachment_data) > SecurityConfig.max_attachment_size:
        logging.warning(f"Attachment exceeds recommended size ({len(attachment_data)} bytes)")
        warnings_generated = True
        
    # Check if content type is potentially problematic
    if content_type.lower() in SecurityConfig.potentially_dangerous_types:
        logging.warning(f"Potentially problematic attachment type: {content_type}")
        warnings_generated = True
        
    # Perform basic security scan if enabled
    if SecurityConfig.scan_attachments:
        # Check for executable signatures
        if attachment_data[:2] == b'MZ':  # Windows executable
            logging.warning("Attachment detected as possible Windows executable")
            warnings_generated = True
            
        # Check for script signatures
        if attachment_data.startswith(b'#!/') or attachment_data.startswith(b'<?php'):
            logging.warning("Attachment detected as possible script file")
            warnings_generated = True
            
    return not warnings_generated

def setup_attachment_dir(output_dir: str) -> str:
    """Set up the directory for saving attachments.
    
    Args:
        output_dir: Base directory for output
        
    Returns:
        Path to the attachment directory
    """
    save_dir = os.path.join(output_dir, 'attachments')
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    return save_dir

def setup_database_connection(db_path: Optional[str] = None) -> Tuple[sqlite3.Connection, bool]:
    """Set up a database connection.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        Tuple of (database connection, whether to close the connection later)
    """
    if db_path is None:
        # Create a default database if none provided
        create_db()
        connection = sqlite3.connect('emaildb.sqlite3')
        return connection, True
    
    return db_path, False

def extract_email_details(message: Any) -> Tuple[str, str, str, str, str, str]:
    """Extract basic email details from a message.
    
    Args:
        message: Email message object
        
    Returns:
        Tuple of (subject, sender_name, sender_email, receiver_name, receiver_email, date)
    """
    subject = message.get('subject', '')
    sender_info = message.get('from', '')
    date = message.get('date', '')
    receiver_info = message.get('to', '')
    
    sender_name, sender_email = email.utils.parseaddr(sender_info)
    receiver_name, receiver_email = email.utils.parseaddr(receiver_info)
    
    # Check for sensitive content in subject (for warning logs only)
    if check_sensitive_content(subject):
        logging.info("Potentially sensitive content detected in email subject")
    
    return subject, sender_name, sender_email, receiver_name, receiver_email, date

def has_required_fields(subject: str, sender_info: str, receiver_info: str) -> bool:
    """Check if the email has all required fields.
    
    Args:
        subject: Email subject
        sender_info: Sender information
        receiver_info: Receiver information
        
    Returns:
        Whether the email has all required fields
    """
    return bool(subject and sender_info and receiver_info)

def save_attachment(attachment_data: bytes, attachment_path: str) -> bool:
    """Save an attachment to disk.
    
    Args:
        attachment_data: Attachment data
        attachment_path: Path to save the attachment
        
    Returns:
        Whether the attachment was saved successfully
    """
    try:
        with open(attachment_path, 'wb') as file:
            file.write(attachment_data)
        return True
    except IOError as e:
        logging.error(f"Failed to save attachment {os.path.basename(attachment_path)}: {e}")
        return False

def create_email_data(subject: str, sender_name: str, sender_email: str, 
                     receiver_name: str, receiver_email: str, date: str,
                     attachment_name: str = "", content_type: str = "",
                     source_pst: str = "") -> Dict[str, str]:
    """Create a dictionary of email data.
    
    Args:
        subject: Email subject
        sender_name: Sender name
        sender_email: Sender email
        receiver_name: Receiver name
        receiver_email: Receiver email
        date: Email date
        attachment_name: Attachment name
        content_type: Attachment content type
        source_pst: Source PST file name
        
    Returns:
        Dictionary of email data
    """
    return {
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

def process_message_attachments(message: Any, save_dir: str, 
                              subject: str, sender_name: str, sender_email: str,
                              receiver_name: str, receiver_email: str, date: str,
                              source_pst: str, db_connection: Any) -> List[Dict[str, str]]:
    """Process attachments in a message.
    
    Args:
        message: Email message object
        save_dir: Directory to save attachments
        subject: Email subject
        sender_name: Sender name
        sender_email: Sender email
        receiver_name: Receiver name
        receiver_email: Receiver email
        date: Email date
        source_pst: Source PST file name
        db_connection: Database connection
        
    Returns:
        List of dictionaries containing email details
    """
    data = []
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
            # Security checks - warnings only, don't block
            check_attachment(content_type, attachment_data)
            
            # Sanitize filename for path safety only
            original_name = attachment_name
            safe_name = sanitize_filename(attachment_name)
            if original_name != safe_name:
                logging.info(f"Sanitized attachment filename for path safety: {original_name} -> {safe_name}")
                attachment_name = safe_name
            
            attachment_path = os.path.join(save_dir, attachment_name)
            if save_attachment(attachment_data, attachment_path):
                logging.info(f"Saved attachment: {attachment_name}")
                
                email_data = create_email_data(
                    subject, sender_name, sender_email, 
                    receiver_name, receiver_email, date,
                    attachment_name, content_type, source_pst
                )
                data.append(email_data)
                store_data(email_data, db_connection)
    
    # If no attachments, still store the email details
    if not has_attachments:
        email_data = create_email_data(
            subject, sender_name, sender_email, 
            receiver_name, receiver_email, date,
            source_pst=source_pst
        )
        data.append(email_data)
        store_data(email_data, db_connection)
    
    return data

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
    save_dir = setup_attachment_dir(output_dir)
    
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
        
        # Get the mailbox object - handle both test mocks and real files
        mbox = mailbox.mbox(mbox_file)
        logging.info(f"Processing {len(mbox)} messages from {mbox_file}")
        
        for message in mbox:
            # Extract email details
            subject, sender_name, sender_email, receiver_name, receiver_email, date = extract_email_details(message)
            
            if not has_required_fields(subject, sender_name, receiver_name):
                logging.warning("Skipping message with missing fields.")
                continue
            
            # Process attachments and store data
            message_data = process_message_attachments(
                message, save_dir, subject, sender_name, sender_email,
                receiver_name, receiver_email, date, source_pst, db_connection
            )
            data.extend(message_data)
        
        # Commit the transaction
        db_connection.commit()
        logging.info(f"Processed {len(data)} emails from {mbox_file}")
        
        # Close the mbox file if it's not a mock
        if hasattr(mbox, 'close'):
            mbox.close()
            
    except Exception as e:
        db_connection.rollback()
        logging.error(f"Failed to parse mbox file {mbox_file}: {e}")
        raise
    finally:
        if close_connection:
            db_connection.close()
            
    return data
