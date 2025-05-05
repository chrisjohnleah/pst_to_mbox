import os
import subprocess
import shutil
import logging
import sqlite3
import argparse
import concurrent.futures
from typing import List, Tuple
from mbox_parser import parse_mbox_file
from db_manager import create_db

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def list_mbox_files(target_dir: str) -> List[str]:
    """List all .mbox files in the target directory."""
    mbox_files = []
    for root, _, files in os.walk(target_dir):
        for file in files:
            if os.path.splitext(file)[-1].lower() == '.mbox':
                mbox_path = os.path.join(root, file)
                mbox_files.append(mbox_path)
    return mbox_files

def convert_single_pst(args: Tuple[str, str, str]) -> bool:
    """Convert a single PST/OST file to MBOX format.
    
    Args:
        args: Tuple containing (file_path, file_name, mbox_dir)
        
    Returns:
        bool: Success status
    """
    file_path, file_name, mbox_dir = args
    logging.info(f"Converting {file_name} to MBOX format...")
    command = ["readpst", "-D", "-b", "-o", mbox_dir, file_path]
    try:
        subprocess.run(command, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to convert {file_name}: {e}")
        return False

def pst_to_mbox(target_dir: str, mbox_dir: str, max_workers: int = None) -> None:
    """Convert PST/OST files to MBOX format using parallel processing.
    
    Args:
        target_dir: Directory containing PST/OST files
        mbox_dir: Directory to save MBOX files
        max_workers: Maximum number of worker processes (None = auto)
    """
    if not os.path.exists(mbox_dir):
        os.makedirs(mbox_dir)
    
    # Build a list of pst/ost files to convert
    conversion_tasks = []
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".pst") or file.endswith(".ost"):
                file_path = os.path.join(root, file)
                conversion_tasks.append((file_path, file, mbox_dir))
    
    if not conversion_tasks:
        logging.warning(f"No PST/OST files found in {target_dir}")
        return
    
    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(convert_single_pst, conversion_tasks))
    
    successful = sum(1 for r in results if r)
    failed = len(results) - successful
    
    logging.info(f"Conversion complete: {successful} successful, {failed} failed")

def process_mbox_files(mbox_dir: str, db_path: str, keep_mbox: bool = False) -> None:
    """Process all MBOX files in the directory and store data in the database.
    
    Args:
        mbox_dir: Directory containing MBOX files
        db_path: Path to the SQLite database file
        keep_mbox: Whether to keep MBOX files after processing
    """
    # Create a single database connection for all operations
    db_connection = sqlite3.connect(db_path)
    
    # Ensure database is created
    create_db(db_path)
    
    # List all MBOX files
    mbox_file_list = list_mbox_files(mbox_dir)
    
    if not mbox_file_list:
        logging.warning(f"No MBOX files found in {mbox_dir}")
        db_connection.close()
        return
        
    logging.info(f"Found {len(mbox_file_list)} MBOX files to process")
    
    # Process each MBOX file
    for mbox_file in mbox_file_list:
        logging.info(f"Processing {mbox_file}...")
        try:
            # Pass the database connection to avoid opening/closing for each file
            parse_mbox_file(mbox_file, mbox_dir, db_connection)
        except Exception as e:
            logging.error(f"Failed to parse {mbox_file}: {e}")
    
    # Close the database connection when all files are processed
    db_connection.close()
    
    # Cleanup if requested
    if not keep_mbox:
        try:
            shutil.rmtree(mbox_dir)
            logging.info(f"Cleaned up temporary directory: {mbox_dir}")
        except Exception as e:
            logging.error(f"Failed to remove directory {mbox_dir}: {e}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert PST/OST files to MBOX format and extract email data')
    
    parser.add_argument('--target-dir', default='target_files',
                        help='Directory containing PST/OST files (default: target_files)')
    
    parser.add_argument('--mbox-dir', default='mbox_dir',
                        help='Directory to store MBOX files (default: mbox_dir)')
    
    parser.add_argument('--db-path', default='emaildb.sqlite3',
                        help='Path to SQLite database (default: emaildb.sqlite3)')
    
    parser.add_argument('--max-workers', type=int, default=None,
                        help='Maximum number of worker processes for conversion (default: auto)')
    
    parser.add_argument('--keep-mbox', action='store_true',
                        help='Keep MBOX files after processing (default: False)')
    
    return parser.parse_args()

def main():
    """Main function to orchestrate the conversion and processing."""
    # Parse command line arguments
    args = parse_arguments()
    
    logging.info(f"Starting PST/MBOX conversion with target_dir={args.target_dir}, mbox_dir={args.mbox_dir}")
    
    # Convert PST/OST files to MBOX using parallel processing
    pst_to_mbox(args.target_dir, args.mbox_dir, args.max_workers)
    
    # Process MBOX files and store data in the database
    process_mbox_files(args.mbox_dir, args.db_path, args.keep_mbox)
    
    logging.info("Processing complete")

if __name__ == '__main__':
    main()
