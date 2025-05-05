import sqlite3
import logging
import os
from typing import Dict, Optional, Any, ContextManager, List, Tuple, Iterator
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseError(Exception):
    """Base exception class for database operations."""
    pass

class DatabaseConnectionError(DatabaseError):
    """Exception raised when unable to connect to the database."""
    pass

class DatabaseCreationError(DatabaseError):
    """Exception raised when unable to create the database or tables."""
    pass

class DatabaseWriteError(DatabaseError):
    """Exception raised when unable to write data to the database."""
    pass

class InvalidDataError(DatabaseError):
    """Exception raised when the data provided is invalid or incomplete."""
    pass

@contextmanager
def get_db_connection(db_path: str, timeout: float = 30.0) -> ContextManager[sqlite3.Connection]:
    """Create a context-managed database connection.
    
    Args:
        db_path: Path to the SQLite database file
        timeout: Timeout for acquiring a database lock (seconds)
        
    Returns:
        Context manager yielding a database connection
        
    Raises:
        DatabaseConnectionError: If connection to the database fails
    """
    # Create directory if it doesn't exist
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
        except OSError as e:
            logging.error(f"Failed to create database directory {db_dir}: {e}")
            raise DatabaseConnectionError(f"Failed to create database directory: {e}") from e
            
    try:
        # Set timeout to avoid hanging when database is locked
        conn = sqlite3.connect(db_path, timeout=timeout)
        conn.execute("PRAGMA journal_mode=WAL")  # Use Write-Ahead Logging for better concurrency
        conn.execute("PRAGMA synchronous=NORMAL")  # Balance durability with performance
        conn.execute("PRAGMA cache_size=-10000")  # Use 10MB cache (negative value means KB)
        conn.execute("PRAGMA temp_store=MEMORY")  # Store temp tables in memory
        
        # Improved foreign key support
        conn.execute("PRAGMA foreign_keys=ON")
        
        yield conn
        
    except sqlite3.Error as e:
        logging.error(f"Database connection error for {db_path}: {e}")
        raise DatabaseConnectionError(f"Failed to connect to database: {e}") from e
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def create_db(db_name: str = 'emaildb.sqlite3') -> bool:
    """Create the database and table if they do not exist.
    
    Args:
        db_name: Path to the SQLite database file
        
    Returns:
        True if database and table were created successfully, False otherwise
        
    Raises:
        DatabaseCreationError: If database or table creation fails
    """
    try:
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            
            # Add indexes for frequently queried columns
            create_statements = [
                '''
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
                ''',
                'CREATE INDEX IF NOT EXISTS idx_sender_email ON emails(sender_email)',
                'CREATE INDEX IF NOT EXISTS idx_recipient_email ON emails(recipient_email)',
                'CREATE INDEX IF NOT EXISTS idx_email_date ON emails(email_date)',
                'CREATE INDEX IF NOT EXISTS idx_source_pst ON emails(source_pst)'
            ]
            
            for statement in create_statements:
                try:
                    cursor.execute(statement)
                except sqlite3.Error as e:
                    logging.error(f"Error executing statement '{statement[:30]}...': {e}")
                    raise
                    
            conn.commit()
            logging.info(f"Database and email table created successfully: {db_name}")
            
            # Verify the table exists by querying schema
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='emails'")
            if not cursor.fetchone():
                raise DatabaseCreationError("Table creation failed: emails table not found")
                
        return True
    except sqlite3.Error as e:
        logging.error(f"Error creating database {db_name}: {e}")
        raise DatabaseCreationError(f"Failed to create database: {e}") from e
    except Exception as e:
        logging.error(f"Unexpected error creating database {db_name}: {e}")
        raise DatabaseCreationError(f"Unexpected error creating database: {e}") from e

def validate_email_data(data_dict: Dict[str, str]) -> None:
    """Validate that the provided data contains all required fields.
    
    Args:
        data_dict: Dictionary of email data
        
    Raises:
        InvalidDataError: If the data is missing required fields
    """
    required_keys = [
        'subject', 'sender_name', 'sender_email', 'recipient_name', 
        'recipient_email', 'attachment_filename', 'attachment_type', 'email_date'
    ]
    
    missing_keys = [key for key in required_keys if key not in data_dict]
    
    if missing_keys:
        raise InvalidDataError(f"Data dictionary is missing required keys: {', '.join(missing_keys)}")

def prepare_email_data(data_dict: Dict[str, str]) -> Tuple:
    """Prepare email data for insertion into the database.
    
    Args:
        data_dict: Dictionary of email data
        
    Returns:
        Tuple of data values ready for insertion
    """
    return (
        str(data_dict.get('subject', '') or ''),
        str(data_dict.get('sender_name', '') or ''),
        str(data_dict.get('sender_email', '') or ''),
        str(data_dict.get('recipient_name', '') or ''),
        str(data_dict.get('recipient_email', '') or ''),
        str(data_dict.get('attachment_filename', '') or ''),
        str(data_dict.get('attachment_type', '') or ''),
        str(data_dict.get('email_date', '') or ''),
        str(data_dict.get('source_pst', '') or '')
    )

def store_data(data_dict: Dict[str, str], db_name: str = 'emaildb.sqlite3') -> bool:
    """Store email data in the database.
    
    Args:
        data_dict: Dictionary containing email data
        db_name: Path to the SQLite database file
        
    Returns:
        True if data was stored successfully, False otherwise
        
    Raises:
        InvalidDataError: If the data is missing required fields
        DatabaseWriteError: If writing to the database fails
    """
    try:
        # Validate the data first
        validate_email_data(data_dict)
        
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            
            # Prepare data with proper error handling for each field
            data_to_insert = prepare_email_data(data_dict)

            # Insert data into the table
            insert_sql = '''
            INSERT INTO emails (
                subject, sender_name, sender_email, recipient_name, 
                recipient_email, attachment_filename, attachment_type, 
                email_date, source_pst
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            try:
                cursor.execute(insert_sql, data_to_insert)
                conn.commit()
                logging.debug("Email data inserted in database successfully.")
                return True
            except sqlite3.IntegrityError as e:
                conn.rollback()
                logging.error(f"Integrity error storing data: {e}")
                raise DatabaseWriteError(f"Integrity error: {e}") from e
            except sqlite3.Error as e:
                conn.rollback()
                logging.error(f"Error storing data: {e}")
                raise DatabaseWriteError(f"Database error: {e}") from e
                
    except InvalidDataError as e:
        logging.error(f"Invalid data: {e}")
        return False
    except DatabaseConnectionError as e:
        logging.error(f"Connection error: {e}")
        return False
    except DatabaseWriteError as e:
        logging.error(f"Write error: {e}")
        return False
    except Exception as e:
        logging.error(f"Unexpected error storing data: {e}")
        return False

def store_data_batch(data_list: List[Dict[str, str]], db_name: str = 'emaildb.sqlite3', batch_size: int = 100) -> Tuple[int, int]:
    """Store multiple email data records in batches for better performance.
    
    Args:
        data_list: List of dictionaries containing email data
        db_name: Path to the SQLite database file
        batch_size: Number of records to insert in a single transaction
        
    Returns:
        Tuple of (number of successful inserts, number of failed inserts)
        
    Raises:
        DatabaseConnectionError: If connection to the database fails
    """
    if not data_list:
        return 0, 0
        
    successful = 0
    failed = 0
    
    try:
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            
            # Insert data into the table
            insert_sql = '''
            INSERT INTO emails (
                subject, sender_name, sender_email, recipient_name, 
                recipient_email, attachment_filename, attachment_type, 
                email_date, source_pst
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Process in batches for better performance
            current_batch = []
            
            for data_dict in data_list:
                try:
                    validate_email_data(data_dict)
                    data_to_insert = prepare_email_data(data_dict)
                    current_batch.append(data_to_insert)
                    
                    # Execute batch if we've reached batch_size
                    if len(current_batch) >= batch_size:
                        try:
                            conn.execute('BEGIN TRANSACTION')
                            cursor.executemany(insert_sql, current_batch)
                            conn.commit()
                            successful += len(current_batch)
                            logging.info(f"Batch of {len(current_batch)} emails inserted successfully")
                        except sqlite3.Error as e:
                            conn.rollback()
                            failed += len(current_batch)
                            logging.error(f"Failed to insert batch: {e}")
                        current_batch = []
                        
                except (InvalidDataError, Exception) as e:
                    logging.error(f"Invalid data, skipping: {e}")
                    failed += 1
            
            # Insert any remaining records
            if current_batch:
                try:
                    conn.execute('BEGIN TRANSACTION')
                    cursor.executemany(insert_sql, current_batch)
                    conn.commit()
                    successful += len(current_batch)
                    logging.info(f"Final batch of {len(current_batch)} emails inserted successfully")
                except sqlite3.Error as e:
                    conn.rollback()
                    failed += len(current_batch)
                    logging.error(f"Failed to insert final batch: {e}")
    
    except DatabaseConnectionError as e:
        logging.error(f"Connection error during batch insert: {e}")
        failed += len(data_list) - successful
        
    return successful, failed

def get_email_count(db_name: str = 'emaildb.sqlite3') -> int:
    """Get the total number of emails in the database.
    
    Args:
        db_name: Path to the SQLite database file
        
    Returns:
        Number of emails in the database
        
    Raises:
        DatabaseConnectionError: If connection to the database fails
    """
    try:
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM emails")
            return cursor.fetchone()[0]
    except (DatabaseConnectionError, sqlite3.Error) as e:
        logging.error(f"Error counting emails: {e}")
        return 0

def query_emails(
    db_name: str = 'emaildb.sqlite3', 
    sender: Optional[str] = None,
    recipient: Optional[str] = None,
    source_pst: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    with_attachments: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0
) -> List[Dict[str, str]]:
    """Query emails with various filters.
    
    Args:
        db_name: Path to the SQLite database file
        sender: Filter by sender email (can be partial)
        recipient: Filter by recipient email (can be partial)
        source_pst: Filter by source PST file
        date_from: Filter by date range (start)
        date_to: Filter by date range (end)
        with_attachments: If True, only include emails with attachments
        limit: Maximum number of results to return
        offset: Offset for pagination
        
    Returns:
        List of dictionaries containing email data
        
    Raises:
        DatabaseConnectionError: If connection to the database fails
    """
    try:
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM emails WHERE 1=1"
            params = []
            
            # Add filters
            if sender:
                query += " AND sender_email LIKE ?"
                params.append(f"%{sender}%")
                
            if recipient:
                query += " AND recipient_email LIKE ?"
                params.append(f"%{recipient}%")
                
            if source_pst:
                query += " AND source_pst = ?"
                params.append(source_pst)
                
            if date_from:
                query += " AND email_date >= ?"
                params.append(date_from)
                
            if date_to:
                query += " AND email_date <= ?"
                params.append(date_to)
                
            if with_attachments is not None:
                if with_attachments:
                    query += " AND attachment_filename != ''"
                else:
                    query += " AND attachment_filename = ''"
            
            # Add limit and offset for pagination
            query += " ORDER BY email_date DESC LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            columns = [column[0] for column in cursor.description]
            
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
                
            return results
            
    except (DatabaseConnectionError, sqlite3.Error) as e:
        logging.error(f"Error querying emails: {e}")
        return []

def get_email_stats(db_name: str = 'emaildb.sqlite3') -> Dict[str, Any]:
    """Get statistics about emails in the database.
    
    Args:
        db_name: Path to the SQLite database file
        
    Returns:
        Dictionary of statistics
        
    Raises:
        DatabaseConnectionError: If connection to the database fails
    """
    stats = {
        'total_emails': 0,
        'emails_with_attachments': 0,
        'unique_senders': 0,
        'unique_recipients': 0,
        'pst_files': [],
        'attachment_types': {}
    }
    
    try:
        with get_db_connection(db_name) as conn:
            cursor = conn.cursor()
            
            # Total emails
            cursor.execute("SELECT COUNT(*) FROM emails")
            stats['total_emails'] = cursor.fetchone()[0]
            
            # Emails with attachments
            cursor.execute("SELECT COUNT(*) FROM emails WHERE attachment_filename != ''")
            stats['emails_with_attachments'] = cursor.fetchone()[0]
            
            # Unique senders
            cursor.execute("SELECT COUNT(DISTINCT sender_email) FROM emails")
            stats['unique_senders'] = cursor.fetchone()[0]
            
            # Unique recipients
            cursor.execute("SELECT COUNT(DISTINCT recipient_email) FROM emails")
            stats['unique_recipients'] = cursor.fetchone()[0]
            
            # PST files
            cursor.execute("SELECT DISTINCT source_pst FROM emails WHERE source_pst != ''")
            stats['pst_files'] = [row[0] for row in cursor.fetchall()]
            
            # Attachment types
            cursor.execute("""
                SELECT attachment_type, COUNT(*) 
                FROM emails 
                WHERE attachment_type != '' 
                GROUP BY attachment_type
                ORDER BY COUNT(*) DESC
            """)
            stats['attachment_types'] = {row[0]: row[1] for row in cursor.fetchall()}
            
            return stats
            
    except (DatabaseConnectionError, sqlite3.Error) as e:
        logging.error(f"Error getting email stats: {e}")
        return stats
