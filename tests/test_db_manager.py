import unittest
import os
import sqlite3
import tempfile
from unittest.mock import patch, MagicMock, mock_open

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import (
    create_db, store_data, get_email_count, get_email_stats,
    DatabaseError, DatabaseConnectionError, DatabaseCreationError, 
    DatabaseWriteError, InvalidDataError, get_db_connection
)

class TestDBManager(unittest.TestCase):
    
    def setUp(self):
        # Create a test database file in a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.test_dir, 'test_emaildb.sqlite3')
        
        # Remove the test database if it exists
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    def tearDown(self):
        # Remove the test database
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        
        # Remove the test directory
        if os.path.exists(self.test_dir):
            # Clean up any other files that might be in the directory
            for root, dirs, files in os.walk(self.test_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            # Now remove the empty directory
            os.rmdir(self.test_dir)
    
    @patch('db_manager.get_db_connection')
    def test_create_db(self, mock_get_connection):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn
        
        # Setup cursor for verification query
        mock_cursor.fetchone.return_value = ['emails']
        
        # Test database creation
        self.assertTrue(create_db(self.test_db))
        
        # Verify connection was established
        mock_get_connection.assert_called_once_with(self.test_db)
        
        # Verify CREATE TABLE was executed 
        mock_cursor.execute.assert_any_call("SELECT name FROM sqlite_master WHERE type='table' AND name='emails'")
        
        # Verify indexes were created
        self.assertGreaterEqual(mock_cursor.execute.call_count, 5)  # At least 5 calls (1 table + 4 indexes)
    
    @patch('db_manager.get_db_connection')
    def test_create_db_error(self, mock_get_connection):
        # Setup mock connection to raise error
        mock_get_connection.side_effect = DatabaseConnectionError("Test connection error")
        
        # Test should fail but not raise exception
        with self.assertRaises(DatabaseCreationError):
            create_db(self.test_db)
    
    def test_store_data_integration(self):
        """Integration test for store_data function"""
        # Create the actual database
        create_db(self.test_db)
        
        # Test data with source_pst
        test_data = {
            'subject': 'Test Subject',
            'sender_name': 'Sender Name',
            'sender_email': 'sender@example.com',
            'recipient_name': 'Recipient Name',
            'recipient_email': 'recipient@example.com',
            'attachment_filename': 'test.txt',
            'attachment_type': 'text/plain',
            'email_date': '2023-01-01T12:00:00',
            'source_pst': 'test.pst'
        }
        
        # Store the data
        self.assertTrue(store_data(test_data, self.test_db))
        
        # Check if the data was stored correctly
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM emails")
            row = cursor.fetchone()
            
            # Check each field (skipping the ID which is auto-generated)
            self.assertEqual(row[1], test_data['subject'])
            self.assertEqual(row[2], test_data['sender_name'])
            self.assertEqual(row[3], test_data['sender_email'])
            self.assertEqual(row[4], test_data['recipient_name'])
            self.assertEqual(row[5], test_data['recipient_email'])
            self.assertEqual(row[6], test_data['attachment_filename'])
            self.assertEqual(row[7], test_data['attachment_type'])
            self.assertEqual(row[8], test_data['email_date'])
            self.assertEqual(row[9], test_data['source_pst'])
    
    def test_store_data_missing_keys(self):
        # Create the database
        create_db(self.test_db)
        
        # Test data with missing keys
        test_data = {'subject': 'Test Subject'}
        
        # Attempt to store incomplete data
        self.assertFalse(store_data(test_data, self.test_db))
    
    @patch('db_manager.get_db_connection')
    def test_store_data_error(self, mock_get_connection):
        # Setup mock connection to raise error
        mock_get_connection.side_effect = DatabaseConnectionError("Test connection error")
        
        # Complete test data
        test_data = {
            'subject': 'Test Subject',
            'sender_name': 'Sender Name',
            'sender_email': 'sender@example.com',
            'recipient_name': 'Recipient Name',
            'recipient_email': 'recipient@example.com',
            'attachment_filename': 'test.txt',
            'attachment_type': 'text/plain',
            'email_date': '2023-01-01T12:00:00'
        }
        
        # Test error handling when storing data
        self.assertFalse(store_data(test_data, self.test_db))
            
    def test_store_data_without_source_pst(self):
        # Create the database
        create_db(self.test_db)
        
        # Test data without source_pst
        test_data = {
            'subject': 'Test Subject',
            'sender_name': 'Sender Name',
            'sender_email': 'sender@example.com',
            'recipient_name': 'Recipient Name',
            'recipient_email': 'recipient@example.com',
            'attachment_filename': 'test.txt',
            'attachment_type': 'text/plain',
            'email_date': '2023-01-01T12:00:00'
        }
        
        # Store the data
        self.assertTrue(store_data(test_data, self.test_db))
        
        # Check if the data was stored correctly with empty source_pst
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT source_pst FROM emails")
            row = cursor.fetchone()
            self.assertEqual(row[0], '')  # Empty source_pst field
    
    @patch('db_manager.get_db_connection')
    def test_get_email_count(self, mock_get_connection):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn
        
        # Setup cursor for query result
        mock_cursor.fetchone.return_value = [42]
        
        # Test get_email_count function
        count = get_email_count(self.test_db)
        
        # Verify result
        self.assertEqual(count, 42)
        
        # Verify connection was established
        mock_get_connection.assert_called_once_with(self.test_db)
        
        # Verify query was executed
        mock_cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM emails")
    
    @patch('db_manager.get_db_connection')
    def test_get_email_stats(self, mock_get_connection):
        # Setup mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_conn
        
        # Setup cursor for query results
        mock_cursor.fetchone.side_effect = [
            [100],  # total_emails
            [50],   # emails_with_attachments
            [25],   # unique_senders
            [30]    # unique_recipients
        ]
        mock_cursor.fetchall.side_effect = [
            [('test1.pst',), ('test2.pst',)],  # pst_files
            [('application/pdf', 20), ('image/jpeg', 15)]  # attachment_types
        ]
        
        # Test get_email_stats function
        stats = get_email_stats(self.test_db)
        
        # Verify results
        expected_stats = {
            'total_emails': 100,
            'emails_with_attachments': 50,
            'unique_senders': 25,
            'unique_recipients': 30,
            'pst_files': ['test1.pst', 'test2.pst'],
            'attachment_types': {
                'application/pdf': 20,
                'image/jpeg': 15
            }
        }
        
        # Check stats match expected values
        self.assertEqual(stats, expected_stats)
        
        # Verify connection was established
        mock_get_connection.assert_called_once_with(self.test_db)
        
        # Verify queries were executed (6 total)
        self.assertEqual(mock_cursor.execute.call_count, 6)
    
    @patch('os.makedirs')
    def test_get_db_connection_creates_directory(self, mock_makedirs):
        # Test that get_db_connection creates the directory if it doesn't exist
        dir_path = os.path.join(self.test_dir, 'new_directory')
        db_path = os.path.join(dir_path, 'new_db.sqlite3')
        
        # Path doesn't exist yet
        with patch('os.path.exists', return_value=False):
            # Need to patch the actual sqlite3.connect to avoid creating a real file
            with patch('sqlite3.connect'):
                with get_db_connection(db_path):
                    pass
        
        # Verify the directory was created
        mock_makedirs.assert_called_once_with(dir_path, exist_ok=True)

if __name__ == '__main__':
    unittest.main()
