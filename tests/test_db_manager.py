import unittest
import os
import sqlite3
from unittest.mock import patch, MagicMock

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_manager import create_db, store_data

class TestDBManager(unittest.TestCase):
    
    def setUp(self):
        # Create a test database file
        self.test_db = 'test_emaildb.sqlite3'
        
        # Remove the test database if it exists
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    def tearDown(self):
        # Remove the test database
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
    
    def test_create_db(self):
        # Test database creation
        self.assertTrue(create_db(self.test_db))
        
        # Check if the file was created
        self.assertTrue(os.path.exists(self.test_db))
        
        # Check if the table was created with correct schema
        with sqlite3.connect(self.test_db) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(emails)")
            columns = [column[1] for column in cursor.fetchall()]
            
            expected_columns = [
                'id', 'subject', 'sender_name', 'sender_email', 
                'recipient_name', 'recipient_email', 'attachment_filename', 
                'attachment_type', 'email_date', 'source_pst'
            ]
            
            for column in expected_columns:
                self.assertIn(column, columns, f"Column '{column}' not found in the table")
    
    @patch('logging.error')
    def test_create_db_error(self, mock_logging):
        # Test error handling when creating the database
        with patch('sqlite3.connect', side_effect=sqlite3.Error("Test error")):
            self.assertFalse(create_db(self.test_db))
            mock_logging.assert_called_once()
    
    def test_store_data(self):
        # Create the database
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
    
    @patch('logging.error')
    def test_store_data_error(self, mock_logging):
        # Create the database
        create_db(self.test_db)
        
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
        with patch('sqlite3.connect', side_effect=sqlite3.Error("Test error")):
            self.assertFalse(store_data(test_data, self.test_db))
            mock_logging.assert_called_once()
            
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

if __name__ == '__main__':
    unittest.main()
