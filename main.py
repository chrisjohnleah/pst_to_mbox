import os
import subprocess
import shutil
import logging
import sqlite3
import argparse
import concurrent.futures
import time
from typing import List, Tuple, Optional, Dict, Any
from pathlib import Path
from datetime import datetime
from mbox_parser import parse_mbox_file
from db_manager import create_db, get_email_stats, get_email_count

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

def find_all_mbox_files(mbox_dir: str) -> List[str]:
    """Find all MBOX files in a directory and its subdirectories.
    
    Args:
        mbox_dir: Directory to search for MBOX files
        
    Returns:
        List of paths to MBOX files
    """
    all_mbox_files = []
    for root, _, files in os.walk(mbox_dir):
        for file in files:
            if file.endswith('.mbox'):
                all_mbox_files.append(os.path.join(root, file))
    
    return all_mbox_files

def determine_source_pst(mbox_file_path: str) -> str:
    """Determine the source PST file from an MBOX file path.
    
    Args:
        mbox_file_path: Path to an MBOX file
        
    Returns:
        Name of the source PST file, or empty string if unknown
    """
    mbox_path_parts = os.path.normpath(mbox_file_path).split(os.sep)
    if len(mbox_path_parts) > 1:
        # The parent directory name should be the PST name
        return f"{mbox_path_parts[-2]}.pst"
    return ""

def clean_up_directory(directory: str) -> None:
    """Remove a directory and log the result.
    
    Args:
        directory: Directory to remove
    """
    try:
        shutil.rmtree(directory)
        logging.info(f"Cleaned up temporary directory: {directory}")
    except Exception as e:
        logging.error(f"Failed to remove directory {directory}: {e}")

def process_with_shared_db(mbox_dir: str, db_path: str, keep_mbox: bool) -> None:
    """Process all MBOX files with a single shared database.
    
    Args:
        mbox_dir: Directory containing MBOX files
        db_path: Path to SQLite database file
        keep_mbox: Whether to keep MBOX files after processing
    """
    # Create a single database connection for all operations
    db_connection = sqlite3.connect(db_path)
    
    # Ensure database is created
    create_db(db_path)
    
    # Find all MBOX files
    all_mbox_files = find_all_mbox_files(mbox_dir)
    
    if not all_mbox_files:
        logging.warning(f"No MBOX files found in {mbox_dir}")
        db_connection.close()
        return
    
    logging.info(f"Found {len(all_mbox_files)} MBOX files to process")
    
    # Process each MBOX file
    for mbox_file in all_mbox_files:
        # Determine source PST
        source_pst = determine_source_pst(mbox_file)
        
        logging.info(f"Processing {mbox_file} from {source_pst}...")
        try:
            # Pass the database connection to avoid opening/closing for each file
            parse_mbox_file(mbox_file, os.path.dirname(mbox_file), db_connection, source_pst)
        except Exception as e:
            logging.error(f"Failed to parse {mbox_file}: {e}")
    
    # Close the database connection when all files are processed
    db_connection.close()
    
    # Cleanup if requested
    if not keep_mbox:
        clean_up_directory(mbox_dir)

def process_with_separate_dbs(mbox_dir: str, db_path: str, keep_mbox: bool, pst_files: List[str]) -> None:
    """Process MBOX files with separate databases per PST file.
    
    Args:
        mbox_dir: Directory containing MBOX files
        db_path: Directory for SQLite database files
        keep_mbox: Whether to keep MBOX files after processing
        pst_files: List of PST files that were converted
    """
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
        
        process_with_separate_dbs(mbox_dir, db_path, keep_mbox, pst_files)
    else:
        # Use a single database for all MBOX files (OPTIONAL)
        process_with_shared_db(mbox_dir, db_path, keep_mbox)

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
        clean_up_directory(pst_mbox_dir)

def get_attachment_sizes(attachment_dir: str) -> Dict[str, int]:
    """Get sizes of all attachments in the directory.
    
    Args:
        attachment_dir: Directory containing attachments
        
    Returns:
        Dictionary of {filename: size_in_bytes}
    """
    attachment_sizes = {}
    if not os.path.exists(attachment_dir):
        return attachment_sizes
        
    for root, _, files in os.walk(attachment_dir):
        for file in files:
            file_path = os.path.join(root, file)
            attachment_sizes[file] = os.path.getsize(file_path)
            
    return attachment_sizes

def format_size(size_bytes: int) -> str:
    """Format size in bytes to human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def format_time(seconds: float) -> str:
    """Format time in seconds to human-readable format.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Human-readable time string
    """
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def collect_conversion_statistics(start_time: float, pst_files: List[str], db_path: str, shared_db: bool) -> Dict[str, Any]:
    """Collect statistics about the conversion process.
    
    Args:
        start_time: Timestamp when conversion started
        pst_files: List of successfully converted PST files
        db_path: Path to database file or directory
        shared_db: Whether a shared database was used
        
    Returns:
        Dictionary of statistics
    """
    stats = {
        'conversion_time': time.time() - start_time,
        'pst_files_converted': len(pst_files),
        'pst_file_names': pst_files,
        'total_emails': 0,
        'emails_with_attachments': 0,
        'unique_senders': 0,
        'unique_recipients': 0,
        'attachment_types': {},
        'attachment_count': 0,
        'attachment_total_size': 0,
        'largest_attachment': {'name': '', 'size': 0},
        'database_size': 0
    }
    
    # Get attachment statistics
    attachment_dir = os.path.join(os.path.dirname(db_path), 'attachments')
    attachment_sizes = get_attachment_sizes(attachment_dir)
    stats['attachment_count'] = len(attachment_sizes)
    
    if attachment_sizes:
        stats['attachment_total_size'] = sum(attachment_sizes.values())
        largest_attachment = max(attachment_sizes.items(), key=lambda x: x[1])
        stats['largest_attachment'] = {
            'name': largest_attachment[0],
            'size': largest_attachment[1]
        }
    
    # Get database statistics
    if shared_db:
        # Single database
        db_size = os.path.getsize(db_path) if os.path.exists(db_path) else 0
        stats['database_size'] = db_size
        
        # Get email statistics
        email_stats = get_email_stats(db_path)
        stats.update(email_stats)
    else:
        # Multiple databases, one per PST
        total_db_size = 0
        combined_stats = {
            'total_emails': 0,
            'emails_with_attachments': 0,
            'unique_senders': set(),
            'unique_recipients': set(),
            'attachment_types': {},
            'pst_files': []
        }
        
        for pst_file in pst_files:
            pst_name = os.path.splitext(pst_file)[0]
            pst_db_path = os.path.join(db_path, f"{pst_name}.sqlite3")
            
            if os.path.exists(pst_db_path):
                db_size = os.path.getsize(pst_db_path)
                total_db_size += db_size
                
                # Get email statistics for this PST
                pst_stats = get_email_stats(pst_db_path)
                
                combined_stats['total_emails'] += pst_stats['total_emails']
                combined_stats['emails_with_attachments'] += pst_stats['emails_with_attachments']
                combined_stats['unique_senders'].update(pst_stats.get('unique_senders', []))
                combined_stats['unique_recipients'].update(pst_stats.get('unique_recipients', []))
                combined_stats['pst_files'].extend(pst_stats.get('pst_files', []))
                
                # Combine attachment types
                for att_type, count in pst_stats.get('attachment_types', {}).items():
                    if att_type in combined_stats['attachment_types']:
                        combined_stats['attachment_types'][att_type] += count
                    else:
                        combined_stats['attachment_types'][att_type] = count
        
        stats['database_size'] = total_db_size
        stats['total_emails'] = combined_stats['total_emails']
        stats['emails_with_attachments'] = combined_stats['emails_with_attachments']
        stats['unique_senders'] = len(combined_stats['unique_senders'])
        stats['unique_recipients'] = len(combined_stats['unique_recipients'])
        stats['attachment_types'] = combined_stats['attachment_types']
    
    return stats

def display_conversion_summary(stats: Dict[str, Any]) -> None:
    """Display a summary of the conversion process.
    
    Args:
        stats: Dictionary of statistics
    """
    print("\n" + "="*80)
    print(" CONVERSION SUMMARY ".center(80, "="))
    print("="*80)
    
    # Processing time
    print(f"Total processing time: {format_time(stats['conversion_time'])}")
    
    # PST files
    print(f"\nPST files converted: {stats['pst_files_converted']}")
    if stats['pst_files_converted'] > 0 and len(stats['pst_file_names']) <= 10:
        for pst_file in stats['pst_file_names']:
            print(f"  - {pst_file}")
    
    # Email statistics
    print(f"\nTotal emails processed: {stats['total_emails']}")
    print(f"Emails with attachments: {stats['emails_with_attachments']} " + 
          f"({stats['emails_with_attachments']/stats['total_emails']*100:.1f}% of total)" if stats['total_emails'] > 0 else "")
    print(f"Unique senders: {stats['unique_senders']}")
    print(f"Unique recipients: {stats['unique_recipients']}")
    
    # Attachment statistics
    print(f"\nTotal attachments: {stats['attachment_count']}")
    print(f"Total attachment size: {format_size(stats['attachment_total_size'])}")
    
    if stats['largest_attachment']['name']:
        print(f"Largest attachment: {stats['largest_attachment']['name']} " + 
              f"({format_size(stats['largest_attachment']['size'])})")
    
    # Attachment types
    if stats['attachment_types']:
        print("\nAttachment types:")
        # Sort by count (descending)
        sorted_types = sorted(stats['attachment_types'].items(), key=lambda x: x[1], reverse=True)
        for att_type, count in sorted_types[:10]:  # Show top 10
            print(f"  - {att_type}: {count}")
        if len(sorted_types) > 10:
            print(f"  - ... and {len(sorted_types) - 10} more types")
    
    # Database size
    print(f"\nDatabase size: {format_size(stats['database_size'])}")
    
    print("="*80)
    print("\nConversion completed successfully!")
    print(f"Emails are available in the database and attachments in the 'attachments' directory.")
    print("="*80 + "\n")

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
    # Record start time
    start_time = time.time()
    
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
    
    # Collect and display statistics
    stats = collect_conversion_statistics(start_time, pst_files, args.db_path, args.shared_db)
    display_conversion_summary(stats)
    
    logging.info("Processing complete")

if __name__ == '__main__':
    main()
