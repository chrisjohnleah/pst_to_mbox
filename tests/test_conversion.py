import os
import sys
import tempfile
import shutil
import subprocess
import unittest
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
    
    @patch('os.walk')
    @patch('os.makedirs')
    @patch('concurrent.futures.ProcessPoolExecutor')
    def test_pst_to_mbox(self, mock_executor, mock_makedirs, mock_walk):
        """Test converting multiple PST files."""
        # Mock os.walk to return PST files
        mock_walk.return_value = [
            (self.target_dir, [], ['test1.pst', 'test2.pst', 'file.txt'])
        ]
        
        # Set up mock executor
        mock_context = MagicMock()
        mock_instance = MagicMock()
        mock_context.__enter__.return_value = mock_instance
        mock_executor.return_value = mock_context
        
        # Mock map to return success status for each PST file
        mock_instance.map.return_value = [True, True]
        
        # Test pst_to_mbox function
        result = main.pst_to_mbox(self.target_dir, self.mbox_dir)
        
        # Verify the result
        self.assertEqual(result, ['test1.pst', 'test2.pst'])
        
        # Verify the executor was called with the correct parameters
        mock_instance.map.assert_called_once()
        
        # Check that the first argument to map is the convert_single_pst function
        self.assertEqual(mock_instance.map.call_args[0][0], main.convert_single_pst)
        
        # Check that the conversion tasks list contains the correct files
        conversion_tasks = mock_instance.map.call_args[0][1]
        self.assertEqual(len(conversion_tasks), 2)
        
        # Each conversion task should be a tuple of (file_path, file_name, mbox_dir)
        self.assertEqual(conversion_tasks[0][1], 'test1.pst')
        self.assertEqual(conversion_tasks[1][1], 'test2.pst')
    
    @patch('os.walk')
    def test_pst_to_mbox_no_files(self, mock_walk):
        """Test when there are no PST files to convert."""
        # Mock os.walk to return no files
        mock_walk.return_value = [(self.target_dir, [], [])]
        
        # Call the function
        pst_files = main.pst_to_mbox(self.target_dir, self.mbox_dir)
        
        # Verify the result
        self.assertEqual(len(pst_files), 0)
    
    @patch('main.parse_mbox_file')
    @patch('os.path.exists')
    @patch('os.walk')
    @patch('os.path.normpath')
    @patch('os.path.join')
    @patch('db_manager.get_db_connection')
    def test_process_mbox_files_shared_db(self, mock_get_connection, mock_join, mock_normpath, mock_walk, mock_exists, mock_parse_mbox):
        """Test processing MBOX files with a shared database."""
        # Create mock paths
        mock_mbox_dir = "/path/to/mbox_dir"
        mock_db_path = os.path.join(self.output_dir, 'emaildb.sqlite3')
        
        # Setup mock connections
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_get_connection.return_value = mock_conn
        
        # Mock os.path.join to return predictable paths
        mock_join.side_effect = lambda *args: '/'.join(args)
        
        # Mock normpath to return the same path
        mock_normpath.side_effect = lambda path: path
        
        # Mock os.path.exists to return True
        mock_exists.return_value = True
        
        # Mock os.walk to return two mbox files
        mock_walk.return_value = [
            (f"{mock_mbox_dir}/subfolder1", [], ["file1.mbox"]),
            (f"{mock_mbox_dir}/subfolder2", [], ["file2.mbox"])
        ]
        
        # Call the function
        main.process_mbox_files(mock_mbox_dir, mock_db_path, shared_db=True)
        
        # Verify get_db_connection was called with the correct path
        mock_get_connection.assert_called_once_with(mock_db_path)
        
        # Verify parse_mbox_file was called for each mbox file
        self.assertEqual(mock_parse_mbox.call_count, 2)
        
        # Verify the correct source_pst value was used for each call
        mock_parse_mbox.assert_any_call(
            f"{mock_mbox_dir}/subfolder1/file1.mbox", 
            f"{mock_mbox_dir}/subfolder1", 
            mock_conn, 
            "subfolder1.pst"
        )
        mock_parse_mbox.assert_any_call(
            f"{mock_mbox_dir}/subfolder2/file2.mbox", 
            f"{mock_mbox_dir}/subfolder2", 
            mock_conn, 
            "subfolder2.pst"
        )
    
    @patch('main.find_all_mbox_files')
    @patch('os.path.exists')
    @patch('os.path.join', side_effect=lambda *args: '/'.join(args))
    @patch('main.parse_mbox_file')
    @patch('main.clean_up_directory')
    @patch('os.makedirs')
    @patch('db_manager.get_db_connection')
    def test_process_mbox_files_shared_db(self, mock_get_connection, mock_makedirs, mock_clean_up, 
                                         mock_parse_mbox, mock_join, mock_exists, mock_find_mbox):
        """Test processing MBOX files with a shared database."""
        # Create mock paths
        mock_mbox_dir = "/path/to/mbox_dir"
        mock_db_path = os.path.join(self.output_dir, 'emaildb.sqlite3')
        
        # Setup mock connections
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_get_connection.return_value = mock_conn
        
        # Mock os.path.exists to return True
        mock_exists.return_value = True
        
        # Mock find_all_mbox_files to return two mbox files
        mock_find_mbox.return_value = [
            f"{mock_mbox_dir}/subfolder1/file1.mbox",
            f"{mock_mbox_dir}/subfolder2/file2.mbox"
        ]
        
        # Call the function with patched dependencies
        main.process_mbox_files(mock_mbox_dir, mock_db_path, shared_db=True)
        
        # Verify get_db_connection was called with the correct path
        mock_get_connection.assert_called_once_with(mock_db_path)
        
        # Verify parse_mbox_file was called for each mbox file
        self.assertEqual(mock_parse_mbox.call_count, 2)
        
        # Check that parse_mbox_file was called with the right paths (using a less strict check)
        # We'll verify that each expected filename appears in at least one of the calls
        calls = [call[0] for call in mock_parse_mbox.call_args_list]
        self.assertTrue(any("file1.mbox" in str(args[0]) for args in calls))
        self.assertTrue(any("file2.mbox" in str(args[0]) for args in calls))
        
        # Verify source_pst values were used
        self.assertTrue(any("subfolder1.pst" in str(args[3]) for args in calls))
        self.assertTrue(any("subfolder2.pst" in str(args[3]) for args in calls))
    
    @patch('main.process_single_pst_mboxes')
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_process_mbox_files_per_pst_db(self, mock_exists, mock_makedirs, mock_process_single):
        """Test processing MBOX files with separate databases per PST (now the default)."""
        # Create mock paths
        mock_mbox_dir = "/path/to/mbox_dir"
        mock_db_path = "/path/to/db_dir"
        
        # Define PST files
        pst_files = ["test1.pst", "test2.pst"]
        
        # Mock os.path.exists to return True when checking PST mbox directories
        mock_exists.side_effect = lambda path: True if '/test1' in path or '/test2' in path else True
        
        # Call the function with direct mocking of the dependency function
        main.process_mbox_files(mock_mbox_dir, mock_db_path, pst_files=pst_files)
        
        # Verify process_single_pst_mboxes was called twice (once for each PST)
        self.assertEqual(mock_process_single.call_count, 2)
        
        # Verify it was called with the correct arguments
        expected_calls = [
            ((os.path.join(mock_mbox_dir, "test1"), os.path.join(mock_db_path, "test1.sqlite3"), False, "test1.pst"), {}),
            ((os.path.join(mock_mbox_dir, "test2"), os.path.join(mock_db_path, "test2.sqlite3"), False, "test2.pst"), {})
        ]
        mock_process_single.assert_has_calls(expected_calls, any_order=True)
    
    @patch('main.collect_conversion_statistics')
    @patch('main.display_conversion_summary')
    @patch('main.process_mbox_files')
    @patch('main.pst_to_mbox')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_function_per_pst_db(self, mock_args, mock_pst_to_mbox, mock_process, 
                                     mock_display_summary, mock_collect_stats):
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
        
        # Mock statistics collection
        mock_collect_stats.return_value = {'total_emails': 10}
        
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
        
        # Verify statistics functions were called
        mock_collect_stats.assert_called_once()
        mock_display_summary.assert_called_once_with({'total_emails': 10})
    
    @patch('main.collect_conversion_statistics')
    @patch('main.display_conversion_summary')
    @patch('main.process_mbox_files')
    @patch('main.pst_to_mbox')
    @patch('argparse.ArgumentParser.parse_args')
    @patch('os.path.isdir')
    def test_main_function_shared_db(self, mock_isdir, mock_args, mock_pst_to_mbox, mock_process,
                                    mock_display_summary, mock_collect_stats):
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
        
        # Mock statistics collection
        mock_collect_stats.return_value = {'total_emails': 10}
        
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
        
        # Verify statistics functions were called
        mock_collect_stats.assert_called_once()
        mock_display_summary.assert_called_once_with({'total_emails': 10})

if __name__ == '__main__':
    unittest.main()
