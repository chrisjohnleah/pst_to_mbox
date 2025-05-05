import os
import sys
import tempfile
import shutil
import subprocess
from unittest.mock import patch, MagicMock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main

class TestConversion(unittest.TestCase):
    """Tests for the PST to MBOX conversion functionality."""
    
    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for testing
        self.test_dir = tempfile.mkdtemp()
        
        # Create subdirectories
        self.target_dir = os.path.join(self.test_dir, 'target_files')
        self.mbox_dir = os.path.join(self.test_dir, 'mbox_dir')
        self.output_dir = os.path.join(self.test_dir, 'output')
        os.makedirs(self.target_dir, exist_ok=True)
        os.makedirs(self.mbox_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Create a dummy PST file
        with open(os.path.join(self.target_dir, 'test.pst'), 'wb') as f:
            f.write(b'dummy content')
    
    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        shutil.rmtree(self.test_dir)
    
    @patch('subprocess.run')
    def test_convert_single_pst(self, mock_run):
        """Test converting a single PST file."""
        # Set up the mock
        mock_run.return_value = subprocess.CompletedProcess(args=[], returncode=0)
        
        # Test data
        file_path = os.path.join(self.target_dir, 'test.pst')
        file_name = 'test.pst'
        
        # Call the function
        result = main.convert_single_pst((file_path, file_name, self.mbox_dir))
        
        # Verify the result
        self.assertTrue(result)
        
        # Verify the command was called correctly
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        self.assertEqual(args[0], 'readpst')
        self.assertEqual(args[4], os.path.join(self.mbox_dir, 'test'))
        self.assertEqual(args[5], file_path)
    
    @patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, []))
    def test_convert_single_pst_error(self, mock_run):
        """Test error handling when converting a PST file."""
        # Test data
        file_path = os.path.join(self.target_dir, 'test.pst')
        file_name = 'test.pst'
        
        # Call the function
        result = main.convert_single_pst((file_path, file_name, self.mbox_dir))
        
        # Verify the result
        self.assertFalse(result)
    
    @patch('main.convert_single_pst')
    def test_pst_to_mbox(self, mock_convert):
        """Test converting multiple PST files."""
        # Set up the mock
        mock_convert.return_value = True
        
        # Create another dummy PST file
        with open(os.path.join(self.target_dir, 'test2.pst'), 'wb') as f:
            f.write(b'dummy content')
        
        # Call the function
        pst_files = main.pst_to_mbox(self.target_dir, self.mbox_dir)
        
        # Verify the results
        self.assertEqual(len(pst_files), 2)
        self.assertIn('test.pst', pst_files)
        self.assertIn('test2.pst', pst_files)
        
        # Verify conversion was called for each file
        self.assertEqual(mock_convert.call_count, 2)
    
    @patch('os.walk')
    def test_pst_to_mbox_no_files(self, mock_walk):
        """Test when there are no PST files to convert."""
        # Mock os.walk to return no files
        mock_walk.return_value = [(self.target_dir, [], [])]
        
        # Call the function
        pst_files = main.pst_to_mbox(self.target_dir, self.mbox_dir)
        
        # Verify the result
        self.assertEqual(len(pst_files), 0)
    
    @patch('main.list_mbox_files')
    @patch('mbox_parser.parse_mbox_file')
    @patch('db_manager.create_db')
    @patch('sqlite3.connect')
    def test_process_mbox_files_shared_db(self, mock_connect, mock_create_db, mock_parse, mock_list_files):
        """Test processing MBOX files with a shared database."""
        # Set up mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_create_db.return_value = True
        
        mock_list_files.return_value = [
            os.path.join(self.mbox_dir, 'test', 'file1.mbox'),
            os.path.join(self.mbox_dir, 'test', 'file2.mbox')
        ]
        
        # Call the function with shared database (now using shared_db=True instead of one_db_per_pst=False)
        main.process_mbox_files(self.mbox_dir, 'test.db', keep_mbox=True, shared_db=True)
        
        # Verify the database was created once
        mock_create_db.assert_called_once()
        
        # Verify mbox files were parsed
        self.assertEqual(mock_parse.call_count, 2)
        
        # Verify correct source_pst was passed
        for call in mock_parse.call_args_list:
            args, _ = call
            if 'file1.mbox' in args[0]:
                self.assertEqual(args[3], 'test.pst')  # source_pst should be derived from path
    
    @patch('os.path.exists')
    @patch('os.makedirs')
    @patch('main.list_mbox_files')
    @patch('mbox_parser.parse_mbox_file')
    @patch('db_manager.create_db')
    @patch('sqlite3.connect')
    def test_process_mbox_files_per_pst_db(self, mock_connect, mock_create_db, mock_parse, 
                                          mock_list_files, mock_makedirs, mock_exists):
        """Test processing MBOX files with separate databases per PST (now the default)."""
        # Set up mocks
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        mock_create_db.return_value = True
        mock_exists.return_value = True
        
        # Mock different MBOX files for different PSTs
        mock_list_files.side_effect = lambda path: {
            os.path.join(self.mbox_dir, 'test1'): [
                os.path.join(self.mbox_dir, 'test1', 'file1.mbox')
            ],
            os.path.join(self.mbox_dir, 'test2'): [
                os.path.join(self.mbox_dir, 'test2', 'file2.mbox')
            ]
        }.get(path, [])
        
        # List of PST files that were converted
        pst_files = ['test1.pst', 'test2.pst']
        
        # Call the function with default settings (separate DB per PST)
        main.process_mbox_files(
            self.mbox_dir, 
            self.output_dir, 
            keep_mbox=True, 
            shared_db=False,  # default
            pst_files=pst_files
        )
        
        # Verify a database was created for each PST
        self.assertEqual(mock_create_db.call_count, 2)
        
        # Verify correct database paths were used
        db_paths = [call[0][0] for call in mock_create_db.call_args_list]
        self.assertIn(os.path.join(self.output_dir, 'test1.sqlite3'), db_paths)
        self.assertIn(os.path.join(self.output_dir, 'test2.sqlite3'), db_paths)
        
        # Verify correct source_pst was passed to each parse_mbox_file call
        self.assertEqual(mock_parse.call_count, 2)
        for call in mock_parse.call_args_list:
            args, _ = call
            if 'test1' in args[1]:
                self.assertEqual(args[3], 'test1.pst')
            elif 'test2' in args[1]:
                self.assertEqual(args[3], 'test2.pst')
    
    @patch('main.process_mbox_files')
    @patch('main.pst_to_mbox')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_function_per_pst_db(self, mock_args, mock_pst_to_mbox, mock_process):
        """Test the main function with separate database per PST (default)."""
        # Mock command line args
        args = MagicMock()
        args.target_dir = self.target_dir
        args.mbox_dir = self.mbox_dir
        args.db_path = os.path.join(self.output_dir, 'db')
        args.max_workers = 2
        args.keep_mbox = False
        args.shared_db = False  # default behavior
        mock_args.return_value = args
        
        # Mock successful conversion
        mock_pst_to_mbox.return_value = ['test1.pst', 'test2.pst']
        
        # Run the main function
        main.main()
        
        # Verify pst_to_mbox was called
        mock_pst_to_mbox.assert_called_once_with(self.target_dir, self.mbox_dir, 2)
        
        # Verify process_mbox_files was called correctly (default is per-PST databases)
        mock_process.assert_called_once_with(
            self.mbox_dir,
            os.path.join(self.output_dir, 'db'),
            False,  # keep_mbox
            False,  # shared_db (default is false)
            ['test1.pst', 'test2.pst']
        )
    
    @patch('main.process_mbox_files')
    @patch('main.pst_to_mbox')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.isdir')
    def test_main_function_shared_db(self, mock_isdir, mock_args, mock_pst_to_mbox, mock_process):
        """Test the main function with shared database (non-default)."""
        # Mock command line args
        args = MagicMock()
        args.target_dir = self.target_dir
        args.mbox_dir = self.mbox_dir
        args.db_path = os.path.join(self.output_dir, 'db')  # a directory
        args.max_workers = 2
        args.keep_mbox = False
        args.shared_db = True  # use shared DB
        mock_args.return_value = args
        
        # Mock isdir to return True (db_path is a directory)
        mock_isdir.return_value = True
        
        # Mock successful conversion
        mock_pst_to_mbox.return_value = ['test1.pst', 'test2.pst']
        
        # Run the main function
        main.main()
        
        # Verify db_path was converted to a file path for the shared DB
        mock_process.assert_called_once()
        call_args = mock_process.call_args[0]
        
        # Should convert directory path to emaildb.sqlite3 in that directory
        self.assertEqual(call_args[1], os.path.join(self.output_dir, 'db', 'emaildb.sqlite3'))
        
        # Verify other args were passed correctly
        self.assertEqual(call_args[2], False)  # keep_mbox
        self.assertEqual(call_args[3], True)   # shared_db
        self.assertEqual(call_args[4], ['test1.pst', 'test2.pst'])  # pst_files

if __name__ == '__main__':
    unittest.main()
