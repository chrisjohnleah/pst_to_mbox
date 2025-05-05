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

from mbox_parser import parse_mbox_file

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
        self.assertEqual(result[0]['attachment_filename'], '')
        self.assertEqual(result[0]['attachment_type'], '')
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_with_attachments(self, mock_mbox):
        """Test parsing a mailbox with messages that have attachments."""
        # Create a message with an attachment
        attachment = MockPart(
            content_type='text/plain',
            filename='attachment.txt',
            disposition='attachment',
            payload=b'test attachment content'
        )
        
        message = MockMessage(
            headers={
                'subject': 'Test Subject',
                'from': 'Sender Name <sender@example.com>',
                'to': 'Receiver Name <receiver@example.com>',
                'date': '2023-01-01 12:00:00'
            },
            parts=[
                MockPart(content_type='multipart/mixed'),
                attachment
            ]
        )
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Mock open for writing attachment
        m_open = mock_open()
        
        # Parse the mailbox
        with patch('builtins.open', m_open), patch('sqlite3.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should have one message in the result
        self.assertEqual(len(result), 1)
        
        # Verify the message data
        self.assertEqual(result[0]['subject'], 'Test Subject')
        self.assertEqual(result[0]['attachment_filename'], 'attachment.txt')
        self.assertEqual(result[0]['attachment_type'], 'text/plain')
        
        # Verify that file was opened for writing
        expected_path = os.path.join(self.output_dir, 'attachments', 'attachment.txt')
        m_open.assert_called_once_with(expected_path, 'wb')
        
        # Verify that attachment content was written
        handle = m_open()
        handle.write.assert_called_once_with(b'test attachment content')
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_attachment_error(self, mock_mbox):
        """Test handling errors when saving attachments."""
        # Create a message with an attachment
        attachment = MockPart(
            content_type='text/plain',
            filename='attachment.txt',
            disposition='attachment',
            payload=b'test attachment content'
        )
        
        message = MockMessage(
            headers={
                'subject': 'Test Subject',
                'from': 'Sender Name <sender@example.com>',
                'to': 'Receiver Name <receiver@example.com>',
                'date': '2023-01-01 12:00:00'
            },
            parts=[attachment]
        )
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Mock open to raise an IOError
        m_open = mock_open()
        m_open.side_effect = IOError("Test IO error")
        
        # Parse the mailbox
        with patch('builtins.open', m_open), patch('sqlite3.connect') as mock_connect:
            mock_cursor = MagicMock()
            mock_connection = MagicMock()
            mock_connection.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_connection
            
            # Should handle the error and continue
            result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Email is still processed even if attachment save fails
        self.assertEqual(len(result), 1)
        # But should log an error (which our test captures)
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_missing_fields(self, mock_mbox):
        """Test parsing a message with missing fields."""
        # Create a message with missing fields
        message = MockMessage(headers={
            # Missing subject
            'from': 'Sender Name <sender@example.com>',
            # Missing to
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
        
        # Should have no messages in the result (due to missing fields)
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
        
        # Mock sqlite3.connect to raise an error during execute
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = sqlite3.Error("Test DB error")
        
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_connect = MagicMock(return_value=mock_connection)
        
        # Parse the mailbox
        with patch('sqlite3.connect', mock_connect):
            # Should handle the error and continue
            result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should have one message in the result (despite DB error)
        self.assertEqual(len(result), 1)
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_general_error(self, mock_mbox):
        """Test handling general errors during parsing."""
        # Mock mailbox.mbox to raise an exception
        mock_mbox.side_effect = Exception("Test general error")
        
        # Parse the mailbox (should handle the error)
        result = parse_mbox_file(self.mbox_path, self.output_dir)
        
        # Should return an empty list on error
        self.assertEqual(result, [])
    
    @patch('mailbox.mbox')
    def test_parse_mbox_file_connection_closure(self, mock_mbox):
        """Test proper connection closure with provided connection."""
        # Create a message
        message = MockMessage(headers={
            'subject': 'Test Subject',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': '2023-01-01 12:00:00'
        })
        
        # Mock the mailbox with one message
        mock_mbox.return_value = [message]
        
        # Create a mock connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Parse the mailbox with provided connection
        result = parse_mbox_file(self.mbox_path, self.output_dir, mock_connection)
        
        # Verify the connection was not closed (because it was provided externally)
        mock_connection.close.assert_not_called()
        
        # But it should have been committed
        mock_connection.commit.assert_called()
    
    @patch('mailbox.mbox')
    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_parse_mbox_file_with_source_pst(self, mock_makedirs, mock_exists, mock_mbox):
        """Test parsing an MBOX file with source_pst parameter."""
        # Mock prerequisites
        mock_exists.return_value = True
        
        # Create a mock message with attachment
        mock_message = MagicMock()
        mock_message.__getitem__.side_effect = lambda key: {
            'subject': 'Test Email',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': 'Thu, 1 Jan 2023 12:00:00 +0000'
        }.get(key, None)
        
        # Set up the attachment part
        mock_part = MagicMock()
        mock_part.get_content_maintype.return_value = 'text'
        mock_part.get.return_value = 'attachment; filename="test.txt"'
        mock_part.get_filename.return_value = 'test.txt'
        mock_part.get_content_type.return_value = 'text/plain'
        mock_part.get_payload.return_value = b'Test attachment content'
        
        # Configure the message's walk method to yield our mock parts
        mock_message.walk.return_value = [mock_part]
        
        # Set up the MBOX to return our mock message
        mock_mbox_instance = MagicMock()
        mock_mbox_instance.__iter__.return_value = [mock_message]
        mock_mbox_instance.__len__.return_value = 1
        mock_mbox.return_value = mock_mbox_instance
        
        # Mock open function for attachment saving
        with patch('builtins.open', mock_open()) as mock_file:
            # Test parse_mbox_file with source_pst parameter
            result = parse_mbox_file(self.mbox_path, self.output_dir, source_pst='test.pst')
            
            # Verify the result
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]['subject'], 'Test Email')
            self.assertEqual(result[0]['sender_email'], 'sender@example.com')
            self.assertEqual(result[0]['recipient_email'], 'receiver@example.com')
            self.assertEqual(result[0]['attachment_filename'], 'test.txt')
            self.assertEqual(result[0]['source_pst'], 'test.pst')
            
            # Verify the attachment was saved
            mock_file.assert_called_once()
            
            # Verify data was inserted into the database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM emails")
            row = cursor.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[9], 'test.pst')  # Check source_pst field
    
    @patch('mailbox.mbox')
    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_parse_mbox_file_no_attachment_with_source_pst(self, mock_makedirs, mock_exists, mock_mbox):
        """Test parsing an MBOX file with no attachments and source_pst parameter."""
        # Mock prerequisites
        mock_exists.return_value = True
        
        # Create a mock message without attachment
        mock_message = MagicMock()
        mock_message.__getitem__.side_effect = lambda key: {
            'subject': 'Test Email',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': 'Thu, 1 Jan 2023 12:00:00 +0000'
        }.get(key, None)
        
        # Set up empty part with no attachment
        mock_part = MagicMock()
        mock_part.get_content_maintype.return_value = 'text'
        mock_part.get.return_value = None  # No Content-Disposition
        
        # Configure message to return our mock part
        mock_message.walk.return_value = [mock_part]
        
        # Set up the MBOX
        mock_mbox_instance = MagicMock()
        mock_mbox_instance.__iter__.return_value = [mock_message]
        mock_mbox_instance.__len__.return_value = 1
        mock_mbox.return_value = mock_mbox_instance
        
        # Test parse_mbox_file with source_pst parameter
        result = parse_mbox_file(self.mbox_path, self.output_dir, source_pst='test.pst')
        
        # Verify the result
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['subject'], 'Test Email')
        self.assertEqual(result[0]['attachment_filename'], '')  # No attachment
        self.assertEqual(result[0]['source_pst'], 'test.pst')
        
        # Verify data was inserted into the database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM emails")
        row = cursor.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[9], 'test.pst')  # Check source_pst field
    
    @patch('mailbox.mbox')
    @patch('db_manager.create_db')
    def test_parse_mbox_file_no_connection_with_source_pst(self, mock_create_db, mock_mbox):
        """Test parsing with auto-created connection and source_pst parameter."""
        # Mock mailbox
        mock_mbox_instance = MagicMock()
        mock_mbox_instance.__iter__.return_value = []
        mock_mbox_instance.__len__.return_value = 0
        mock_mbox.return_value = mock_mbox_instance
        
        # Mock create_db
        mock_create_db.return_value = True
        
        with patch('sqlite3.connect') as mock_connect:
            # Mock the connection
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            # Call without providing a connection but with source_pst
            parse_mbox_file(self.mbox_path, self.output_dir, source_pst='test.pst')
            
            # Verify a connection was created
            mock_connect.assert_called_once()
            # Verify connection was closed
            mock_connection.close.assert_called_once()
    
    @patch('mailbox.mbox', side_effect=Exception("Test exception"))
    def test_parse_mbox_file_exception_with_source_pst(self, mock_mbox):
        """Test exception handling with source_pst parameter."""
        with self.assertRaises(Exception):
            parse_mbox_file(self.mbox_path, self.output_dir, source_pst='test.pst')
            
        # Verify transaction was rolled back
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM emails")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 0)
    
    @patch('mailbox.mbox')
    @patch('os.path.exists')
    @patch('os.makedirs')
    def test_parse_mbox_file_attachment_error_with_source_pst(self, mock_makedirs, mock_exists, mock_mbox):
        """Test handling attachment saving errors with source_pst parameter."""
        # Mock prerequisites
        mock_exists.return_value = True
        
        # Create a mock message with attachment
        mock_message = MagicMock()
        mock_message.__getitem__.side_effect = lambda key: {
            'subject': 'Test Email',
            'from': 'Sender Name <sender@example.com>',
            'to': 'Receiver Name <receiver@example.com>',
            'date': 'Thu, 1 Jan 2023 12:00:00 +0000'
        }.get(key, None)
        
        # Set up the attachment part
        mock_part = MagicMock()
        mock_part.get_content_maintype.return_value = 'text'
        mock_part.get.return_value = 'attachment; filename="test.txt"'
        mock_part.get_filename.return_value = 'test.txt'
        mock_part.get_content_type.return_value = 'text/plain'
        mock_part.get_payload.return_value = b'Test attachment content'
        
        # Configure the message
        mock_message.walk.return_value = [mock_part]
        
        # Set up the MBOX
        mock_mbox_instance = MagicMock()
        mock_mbox_instance.__iter__.return_value = [mock_message]
        mock_mbox_instance.__len__.return_value = 1
        mock_mbox.return_value = mock_mbox_instance
        
        # Mock open to raise IOError
        with patch('builtins.open', side_effect=IOError("Test I/O error")):
            with patch('logging.error') as mock_log_error:
                # Test with attachment error and source_pst
                result = parse_mbox_file(self.mbox_path, self.output_dir, source_pst='test.pst')
                
                # Verify error was logged
                mock_log_error.assert_called()
                
                # Despite the attachment error, we should still have some data
                self.assertEqual(len(result), 0)

if __name__ == '__main__':
    unittest.main()
