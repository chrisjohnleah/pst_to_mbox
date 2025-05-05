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
    # Create a subdirectory for this PST file's output
    pst_output_dir = os.path.join(mbox_dir, os.path.splitext(file_name)[0])
    os.makedirs(pst_output_dir, exist_ok=True)
    
    logging.info(f"Converting {file_name} to MBOX format...")
    command = ["readpst", "-D", "-b", "-o", pst_output_dir, file_path]
    try:
        subprocess.run(command, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to convert {file_name}: {e}")
        return False

def pst_to_mbox(target_dir: str, mbox_dir: str, max_workers: int = None) -> List[str]:
    """Convert PST/OST files to MBOX format using parallel processing.
    
    Args:
        target_dir: Directory containing PST/OST files
        mbox_dir: Directory to save MBOX files
        max_workers: Maximum number of worker processes (None = auto)
        
    Returns:
        List of PST file names that were successfully converted
    """
    if not os.path.exists(mbox_dir):
        os.makedirs(mbox_dir)
    
    # Build a list of pst/ost files to convert
    conversion_tasks = []
    pst_files = []
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".pst") or file.endswith(".ost"):
                file_path = os.path.join(root, file)
                conversion_tasks.append((file_path, file, mbox_dir))
                pst_files.append(file)
    
    if not conversion_tasks:
        logging.warning(f"No PST/OST files found in {target_dir}")
        return []
    
    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(convert_single_pst, conversion_tasks))
    
    # Filter out unsuccessful conversions
    successful_pst_files = [pst for pst, success in zip(pst_files, results) if success]
    
    successful = len(successful_pst_files)
    failed = len(results) - successful
    
    logging.info(f"Conversion complete: {successful} successful, {failed} failed")
    return successful_pst_files

def process_mbox_files(mbox_dir: str, db_path: str, keep_mbox: bool = False, shared_db: bool = False, pst_files: List[str] = None) -> None:
    """Process all MBOX files in the directory and store data in the database.
    
    Args:
        mbox_dir: Directory containing MBOX files
        db_path: Path to SQLite database file or directory for per-PST databases
        keep_mbox: Whether to keep MBOX files after processing
        shared_db: Whether to use a single shared database for all PST files
        pst_files: List of PST files that were converted
    """
    if not shared_db:
        # Process each PST's MBOX files separately (DEFAULT)
        if not pst_files:
            logging.warning("No PST files list provided for separate database mode")
            return
            
        # Create output directory if it doesn't exist
        os.makedirs(db_path, exist_ok=True)
            
        for pst_file in pst_files:
            pst_name = os.path.splitext(pst_file)[0]
            pst_mbox_dir = os.path.join(mbox_dir, pst_name)
            
            if not os.path.exists(pst_mbox_dir):
                logging.warning(f"MBOX directory for {pst_file} not found: {pst_mbox_dir}")
                continue
                
            # Create a database for this PST file
            pst_db_path = os.path.join(db_path, f"{pst_name}.sqlite3")
            process_single_pst_mboxes(pst_mbox_dir, pst_db_path, keep_mbox, pst_file)
    else:
        # Use a single database for all MBOX files (OPTIONAL)
        # Create a single database connection for all operations
        db_connection = sqlite3.connect(db_path)
        
        # Ensure database is created
        create_db(db_path)
        
        # List all MBOX files across all subdirectories
        all_mbox_files = []
        for root, _, files in os.walk(mbox_dir):
            for file in files:
                if file.endswith('.mbox'):
                    all_mbox_files.append(os.path.join(root, file))
        
        if not all_mbox_files:
            logging.warning(f"No MBOX files found in {mbox_dir}")
            db_connection.close()
            return
            
        logging.info(f"Found {len(all_mbox_files)} MBOX files to process")
        
        # Process each MBOX file
        for mbox_file in all_mbox_files:
            # Determine source PST from path
            mbox_path_parts = os.path.normpath(mbox_file).split(os.sep)
            source_pst = ""
            if len(mbox_path_parts) > 1:
                # The parent directory name should be the PST name
                source_pst = f"{mbox_path_parts[-2]}.pst"
                
            logging.info(f"Processing {mbox_file} from {source_pst}...")
            try:
                # Pass the database connection to avoid opening/closing for each file
                # Pass the source PST name
                parse_mbox_file(mbox_file, os.path.dirname(mbox_file), db_connection, source_pst)
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

def process_single_pst_mboxes(pst_mbox_dir: str, db_path: str, keep_mbox: bool, pst_file: str) -> None:
    """Process MBOX files for a single PST file.
    
    Args:
        pst_mbox_dir: Directory containing MBOX files for this PST
        db_path: Path to SQLite database file for this PST
        keep_mbox: Whether to keep MBOX files after processing
        pst_file: Name of the PST file (for source tracking)
    """
    # Create a database for this PST
    create_db(db_path)
    
    # Create a database connection
    db_connection = sqlite3.connect(db_path)
    
    # List MBOX files for this PST
    mbox_files = list_mbox_files(pst_mbox_dir)
    
    if not mbox_files:
        logging.warning(f"No MBOX files found for {pst_file} in {pst_mbox_dir}")
        db_connection.close()
        return
        
    logging.info(f"Processing {len(mbox_files)} MBOX files from {pst_file}")
    
    # Process each MBOX file
    for mbox_file in mbox_files:
        logging.info(f"Processing {mbox_file}...")
        try:
            # Pass the database connection and source PST
            parse_mbox_file(mbox_file, pst_mbox_dir, db_connection, pst_file)
        except Exception as e:
            logging.error(f"Failed to parse {mbox_file}: {e}")
    
    # Close the database connection
    db_connection.close()
    
    # Cleanup if requested
    if not keep_mbox and not os.path.samefile(pst_mbox_dir, os.path.dirname(db_path)):
        try:
            shutil.rmtree(pst_mbox_dir)
            logging.info(f"Cleaned up temporary directory: {pst_mbox_dir}")
        except Exception as e:
            logging.error(f"Failed to remove directory {pst_mbox_dir}: {e}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Convert PST/OST files to MBOX format and extract email data')
    
    parser.add_argument('--target-dir', default='target_files',
                        help='Directory containing PST/OST files (default: target_files)')
    
    parser.add_argument('--mbox-dir', default='mbox_dir',
                        help='Directory to store MBOX files (default: mbox_dir)')
    
    parser.add_argument('--db-path', default='output/db',
                        help='Path to directory for per-PST databases or a single shared database file (default: output/db)')
    
    parser.add_argument('--max-workers', type=int, default=None,
                        help='Maximum number of worker processes for conversion (default: auto)')
    
    parser.add_argument('--keep-mbox', action='store_true',
                        help='Keep MBOX files after processing (default: False)')
                        
    parser.add_argument('--shared-db', action='store_true',
                        help='Use a single shared database for all PST files (default: False)')
    
    return parser.parse_args()

def main():
    """Main function to orchestrate the conversion and processing."""
    # Parse command line arguments
    args = parse_arguments()
    
    # If using shared db and db_path is a directory, create a default path for the shared db
    if args.shared_db and os.path.isdir(args.db_path):
        args.db_path = os.path.join(args.db_path, 'emaildb.sqlite3')
        logging.info(f"Using shared database file: {args.db_path}")
    
    # Make sure output/attachments exists
    attachments_dir = os.path.join(os.path.dirname(args.db_path), 'attachments')
    os.makedirs(attachments_dir, exist_ok=True)
    
    logging.info(f"Starting PST/MBOX conversion with target_dir={args.target_dir}, mbox_dir={args.mbox_dir}")
    logging.info(f"Database mode: {'Shared' if args.shared_db else 'Per-PST'}, path: {args.db_path}")
    
    # Convert PST/OST files to MBOX using parallel processing
    # Get list of successfully converted PST files
    pst_files = pst_to_mbox(args.target_dir, args.mbox_dir, args.max_workers)
    
    # Process MBOX files and store data in the database(s)
    process_mbox_files(
        args.mbox_dir, 
        args.db_path, 
        args.keep_mbox,
        args.shared_db,
        pst_files
    )
    
    logging.info("Processing complete")

if __name__ == '__main__':
    main()
