import os
import sys
import unittest
import tempfile
import mailbox
import sqlite3
import shutil
from unittest.mock import patch, MagicMock, mock_open

# Add the parent directory to the path so we can import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mbox_parser import (
    parse_mbox_file, setup_attachment_dir, extract_email_details,
    has_required_fields, save_attachment, create_email_data,
    check_sensitive_content, sanitize_filename, check_attachment,
    process_message_attachments
)

class MockMessage:
    """Mock for mailbox.mboxMessage."""
    
    def __init__(self, headers=None, parts=None):
        self.headers = headers or {}
        self.parts = parts or []
    
    def __getitem__(self, key):
        return self.headers.get(key, '')
    
    def get(self, key, default=''):
        return self.headers.get(key, default)
    
    def walk(self):
        return self.parts

class MockPart:
    """Mock for email message parts."""
    
    def __init__(self, content_type='text/plain', filename=None, disposition=None, payload=None):
        self.content_type = content_type
        self.filename = filename
        self.disposition = disposition
        self.payload = payload
    
    def get_content_maintype(self):
        return self.content_type.split('/')[0]
    
    def get_content_type(self):
        return self.content_type
    
    def get(self, key, default=None):
        if key == 'Content-Disposition':
            return self.disposition
        return default
    
    def get_filename(self):
        return self.filename
    
    def get_payload(self, decode=False):
        return self.payload

class TestMboxParser(unittest.TestCase):
    """Tests for the MBOX parser module."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        self.mbox_path = os.path.join(self.test_dir, 'test.mbox')
        self.output_dir = os.path.join(self.test_dir, 'output')
        self.db_path = os.path.join(self.test_dir, 'test.db')
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a test database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
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
        )
        ''')
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        shutil.rmtree(self.test_dir)
    
    def test_setup_attachment_dir(self):
        """Test the setup_attachment_dir function."""
        # Call the function
        save_dir = setup_attachment_dir(self.output_dir)
        
        # Verify the result
        expected_dir = os.path.join(self.output_dir, 'attachments')
        self.assertEqual(save_dir, expected_dir)
        self.assertTrue(os.path.exists(expected_dir))
    
    def test_extract_email_details(self):
        """Test extracting email details from a message."""
        # Create test message
        message = MockMessage(headers={
            'subject': 'Test Subject',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': '2023-01-01 12:00:00'
        })
        
        # Extract details
        subject, sender_name, sender_email, receiver_name, receiver_email, date = extract_email_details(message)
        
        # Verify results
        self.assertEqual(subject, 'Test Subject')
        self.assertEqual(sender_name, 'Sender Name')
        self.assertEqual(sender_email, 'sender@example.com')
        self.assertEqual(receiver_name, 'Receiver Name')
        self.assertEqual(receiver_email, 'receiver@example.com')
        self.assertEqual(date, '2023-01-01 12:00:00')
    
    def test_has_required_fields(self):
        """Test checking for required fields."""
        # Test with all fields present
        self.assertTrue(has_required_fields('Subject', 'Sender', 'Receiver'))
        
        # Test with missing fields
        self.assertFalse(has_required_fields('', 'Sender', 'Receiver'))
        self.assertFalse(has_required_fields('Subject', '', 'Receiver'))
        self.assertFalse(has_required_fields('Subject', 'Sender', ''))
    
    def test_save_attachment(self):
        """Test saving an attachment."""
        # Create a test attachment
        attachment_data = b'Test attachment content'
        attachment_path = os.path.join(self.test_dir, 'attachment.txt')
        
        # Save the attachment
        result = save_attachment(attachment_data, attachment_path)
        
        # Verify the result
        self.assertTrue(result)
        self.assertTrue(os.path.exists(attachment_path))
        
        # Check file content
        with open(attachment_path, 'rb') as f:
            content = f.read()
            self.assertEqual(content, attachment_data)
    
    def test_save_attachment_error(self):
        """Test handling errors when saving attachments."""
        # Create test data
        attachment_data = b'Test attachment content'
        
        # Test with IOError
        with patch('builtins.open', side_effect=IOError("Test error")):
            result = save_attachment(attachment_data, "invalid/path")
            self.assertFalse(result)
    
    def test_create_email_data(self):
        """Test creating email data dictionary."""
        # Create email data
        email_data = create_email_data(
            'Test Subject', 'Sender Name', 'sender@example.com',
            'Receiver Name', 'receiver@example.com', '2023-01-01',
            'test.txt', 'text/plain', 'test.pst'
        )
        
        # Verify the result
        expected = {
            'subject': 'Test Subject',
            'sender_name': 'Sender Name',
            'sender_email': 'sender@example.com',
            'recipient_name': 'Receiver Name',
            'recipient_email': 'receiver@example.com',
            'attachment_filename': 'test.txt',
            'attachment_type': 'text/plain',
            'email_date': '2023-01-01',
            'source_pst': 'test.pst'
        }
        self.assertEqual(email_data, expected)
    
    def test_check_sensitive_content(self):
        """Test checking for sensitive content."""
        # Test with sensitive content
        self.assertTrue(check_sensitive_content("This contains password information"))
        self.assertTrue(check_sensitive_content("CONFIDENTIAL: secret data"))
        self.assertTrue(check_sensitive_content("SSN: 123-45-6789"))
        
        # Test with non-sensitive content
        self.assertFalse(check_sensitive_content("This is a normal message"))
        self.assertFalse(check_sensitive_content(""))
    
    def test_sanitize_filename(self):
        """Test sanitizing filenames."""
        # Test with invalid characters
        self.assertEqual(sanitize_filename("file/with\\invalid:chars"), "with_invalid_chars")
        
        # Test with path traversal attempt
        self.assertEqual(sanitize_filename("../../../etc/passwd"), "passwd")
        
        # Test with normal filename
        self.assertEqual(sanitize_filename("normal_file.txt"), "normal_file.txt")
    
    def test_check_attachment(self):
        """Test checking attachments for security issues."""
        # Test with normal attachment
        normal_data = b'This is normal data'
        self.assertTrue(check_attachment('text/plain', normal_data))
        
        # Test with executable signature
        exe_data = b'MZ\x90\x00\x03\x00\x00\x00'  # Windows executable signature
        self.assertFalse(check_attachment('application/octet-stream', exe_data))
        
        # Test with script signature
        script_data = b'#!/bin/bash\necho "Hello"'
        self.assertFalse(check_attachment('text/plain', script_data))
        
        # Test with potentially problematic content type
        self.assertFalse(check_attachment('application/x-msdownload', b'Normal data'))
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_empty(self, mock_mbox):
        """Test parsing an empty MBOX file."""
        # Mock an empty mailbox
        mock_mbox.return_value = []
        
        # Parse the empty mailbox
        result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should return an empty list
        self.assertEqual(result, [])
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_no_attachments(self, mock_mbox):
        """Test parsing a mailbox with messages but no attachments."""
        # Create a message without attachments
        message = MockMessage(headers={
            'subject': 'Test Subject',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': '2023-01-01 12:00:00'
        })
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Parse the mailbox
        with patch('sqlite3.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should have one message in the result
        self.assertEqual(len(result), 1)
        
        # Verify the message data
        self.assertEqual(result[0]['subject'], 'Test Subject')
        self.assertEqual(result[0]['sender_name'], 'Sender Name')
        self.assertEqual(result[0]['sender_email'], 'sender@example.com')
        self.assertEqual(result[0]['recipient_name'], 'Receiver Name')
        self.assertEqual(result[0]['recipient_email'], 'receiver@example.com')
        self.assertEqual(result[0]['email_date'], '2023-01-01 12:00:00')
        self.assertEqual(result[0]['attachment_filename'], '')  # No attachment
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_with_attachments(self, mock_mbox):
        """Test parsing a mailbox with messages that have attachments."""
        # Create a message with an attachment
        message = MockMessage(
            headers={
                'subject': 'Test Subject with Attachment',
                'from': 'Sender Name <sender@example.com>',
                'to': 'Receiver Name <receiver@example.com>',
                'date': '2023-01-01 12:00:00'
            },
            parts=[
                MockPart(
                    content_type='text/plain', 
                    filename='test.txt', 
                    disposition='attachment; filename="test.txt"',
                    payload=b'Test attachment content'
                )
            ]
        )
        
        # Mock the mailbox with one message
        mock_mailbox = MagicMock()
        mock_mailbox.__iter__.return_value = [message]
        mock_mailbox.__len__.return_value = 1
        mock_mbox.return_value = mock_mailbox
        
        # Set up the test environment
        os.makedirs(os.path.join(self.output_dir, 'attachments'), exist_ok=True)
        
        # Mock the store_data function to prevent database operations
        with patch('mbox_parser.store_data', return_value=True) as mock_store_data:
            # Mock save_attachment to return True
            with patch('mbox_parser.save_attachment', return_value=True) as mock_save:
                # Mock file opening
                with patch('builtins.open', mock_open()) as mock_file:
                    # Parse the mailbox
                    result = parse_mbox_file(self.mbox_path, self.output_dir)
                
                # Verify save_attachment was called
                mock_save.assert_called_once()
            
            # Verify store_data was called twice (once for the attachment info)
            mock_store_data.assert_called_once()
        
        # Should have one message in the result
        self.assertEqual(len(result), 1)
        
        # Verify the message data
        self.assertEqual(result[0]['subject'], 'Test Subject with Attachment')
        self.assertEqual(result[0]['attachment_filename'], 'test.txt')
        self.assertEqual(result[0]['attachment_type'], 'text/plain')
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_attachment_error(self, mock_mbox):
        """Test handling errors when saving attachments."""
        # Create a message with an attachment
        message = MockMessage(
            headers={
                'subject': 'Test Subject with Attachment',
                'from': 'Sender Name <sender@example.com>',
                'to': 'Receiver Name <receiver@example.com>',
                'date': '2023-01-01 12:00:00'
            },
            parts=[
                MockPart(
                    content_type='text/plain', 
                    filename='test.txt', 
                    disposition='attachment; filename="test.txt"',
                    payload=b'Test attachment content'
                )
            ]
        )
        
        # Mock the mailbox with one message
        mock_mailbox = MagicMock()
        mock_mailbox.__iter__.return_value = [message]
        mock_mailbox.__len__.return_value = 1
        mock_mbox.return_value = mock_mailbox
        
        # Mock save_attachment to fail
        with patch('mbox_parser.save_attachment', return_value=False):
            with patch('sqlite3.connect') as mock_connect:
                mock_connection = MagicMock()
                mock_connect.return_value = mock_connection
                
                # Parse the mailbox
                result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should not have any message in the result (since attachment failed)
        self.assertEqual(len(result), 0)
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_missing_fields(self, mock_mbox):
        """Test parsing a message with missing fields."""
        # Create a message with missing fields
        message = MockMessage(headers={
            'subject': '',  # Missing subject
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': '2023-01-01 12:00:00'
        })
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Parse the mailbox
        with patch('sqlite3.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should not have any message in the result (due to missing fields)
        self.assertEqual(len(result), 0)
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_db_error(self, mock_mbox):
        """Test handling database errors during parsing."""
        # Create a message
        message = MockMessage(headers={
            'subject': 'Test Subject',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': '2023-01-01 12:00:00'
        })
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Mock store_data to raise an exception
        with patch('mbox_parser.store_data', side_effect=Exception("Test exception")):
            with patch('sqlite3.connect') as mock_connect:
                mock_connection = MagicMock()
                mock_connect.return_value = mock_connection
                
                # Parse the mailbox should raise the exception
                with self.assertRaises(Exception):
                    parse_mbox_file(self.mbox_path, self.output_dir)
                
                # Verify rollback was called
                mock_connection.rollback.assert_called_once()
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_general_error(self, mock_mbox):
        """Test handling general errors during parsing."""
        # Make mailbox.mbox raise an exception
        mock_mbox.side_effect = Exception("Test exception")
        
        # Parse the mailbox should raise the exception
        with self.assertRaises(Exception):
            parse_mbox_file(self.mbox_path, self.output_dir)
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_connection_closure(self, mock_mbox):
        """Test proper connection closure with provided connection."""
        # Mock an empty mailbox
        mock_mbox.return_value = []
        
        # Create a mock connection
        mock_connection = MagicMock()
        
        # Parse with the provided connection
        parse_mbox_file(self.mbox_path, self.output_dir, mock_connection)
        
        # Verify the connection was not closed (since it was provided)
        mock_connection.close.assert_not_called()
    
    @patch('mailbox.mbox')
    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_parse_mbox_file_with_source_pst(self, mock_makedirs, mock_exists, mock_mbox):
        """Test parsing an MBOX file with source_pst parameter."""
        # Mock prerequisites
        mock_exists.return_value = True
        
        # Create a message with an attachment
        message = MockMessage(
            headers={
                'subject': 'Test Subject with Attachment',
                'from': 'Sender Name <sender@example.com>',
                'to': 'Receiver Name <receiver@example.com>',
                'date': '2023-01-01 12:00:00'
            },
            parts=[
                MockPart(
                    content_type='text/plain', 
                    filename='test.txt', 
                    disposition='attachment; filename="test.txt"',
                    payload=b'Test attachment content'
                )
            ]
        )
        
        # Mock the mailbox with one message
        mock_mailbox = MagicMock()
        mock_mailbox.__iter__.return_value = [message]
        mock_mailbox.__len__.return_value = 1
        mock_mbox.return_value = mock_mailbox
        
        # Set up the database connection
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
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
        )
        ''')
        conn.commit()
        
        # Mock save_attachment to return True and store_data to insert actual data
        with patch('mbox_parser.save_attachment', return_value=True):
            with patch('mbox_parser.store_data') as mock_store_data:
                # Set up mock_store_data to call our helper method
                mock_store_data.side_effect = lambda data, conn_arg: self._mock_store_data(data, conn if conn_arg == conn else conn_arg)
                
                # Parse the mailbox with source_pst
                with patch('builtins.open', mock_open()) as mock_file:
                    result = parse_mbox_file(self.mbox_path, self.output_dir, conn, source_pst='test.pst')
                
                # Should have one message in the result
                self.assertEqual(len(result), 1)
                
                # Verify source_pst was included in the result
                self.assertEqual(result[0]['source_pst'], 'test.pst')
                
                # Verify mock_store_data was called with the right data
                called_data = mock_store_data.call_args[0][0]
                self.assertEqual(called_data['source_pst'], 'test.pst')
                
                # Query the database to verify data was stored
                cursor = conn.cursor()
                cursor.execute("SELECT source_pst FROM emails")
                row = cursor.fetchone()
                self.assertIsNotNone(row, "No data was inserted into the database")
                self.assertEqual(row[0], 'test.pst')
        
        # Clean up
        conn.close()
    
    def _mock_store_data(self, data, db_connection):
        """Helper method to mock store_data by actually inserting data."""
        cursor = db_connection.cursor()
        cursor.execute('''
        INSERT INTO emails (subject, sender_name, sender_email, recipient_name, recipient_email, attachment_filename, attachment_type, email_date, source_pst)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['subject'],
            data['sender_name'],
            data['sender_email'],
            data['recipient_name'],
            data['recipient_email'],
            data['attachment_filename'],
            data['attachment_type'],
            data['email_date'],
            data['source_pst']
        ))
        db_connection.commit()
        return True

if __name__ == '__main__':
    unittest.main()
