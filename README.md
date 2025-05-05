# PST to MBOX Converter

A robust utility for converting Outlook PST/OST files to MBOX format and extracting email data into SQLite databases.

## About

PST to MBOX Converter is a powerful tool designed to convert Microsoft Outlook PST/OST files into the more universally compatible MBOX format. The tool extracts email details, properly organises data by creating separate SQLite databases for each PST file, and saves all attachments in a structured fashion. Built with performance in mind, it utilises parallel processing to efficiently handle multiple PST files simultaneously.

Perfect for email archiving, migration projects, or data analysis purposes where Outlook data needs to be accessed in a platform-independent manner. The tool's modular design makes it easily extensible for various email processing workflows.

## Features

- Converts PST/OST files to MBOX format using the `readpst` utility
- Extracts email details (sender, receiver, subject, date, etc.)
- Creates separate SQLite database per PST file for better data organisation (configurable)
- Extracts and saves email attachments
- Processes multiple PST files in parallel for faster conversion
- Optimised database operations for improved performance

## Prerequisites

- Python 3.6 or higher
- Docker (optional, for containerised usage)
- `readpst` utility (installed automatically when using Docker)

## Installation

### Using Docker (Recommended)

1. Clone the repository:
   ```bash
   git clone https://github.com/chrisjohnleah/pst_to_mbox.git
   cd pst_to_mbox
   ```

2. Build the Docker image:
   ```bash
   docker-compose build
   ```

### Without Docker

1. Clone the repository:
   ```bash
   git clone https://github.com/chrisjohnleah/pst_to_mbox.git
   cd pst_to_mbox
   ```

2. Install the `readpst` utility:
   ```bash
   # Ubuntu/Debian
   sudo apt-get update
   sudo apt-get install -y pst-utils
   ```

## Getting Started

1. Create the required directories:

   ```bash
   mkdir -p target_files mbox_dir output/db output/attachments
   ```

   - `target_files`: Place your PST/OST files here for conversion
   - `mbox_dir`: Temporary directory for MBOX files during conversion
   - `output/db`: Directory for storing separate SQLite databases per PST file
   - `output/attachments`: Storage location for extracted email attachments

2. Place your PST and OST files that you want to convert into the `target_files` directory.

3. Run the conversion process:
   ```bash
   docker-compose up   # If using Docker
   # OR
   python main.py      # If not using Docker
   ```

   This will:
   - Convert your PST/OST files to MBOX format (saved temporarily in `mbox_dir`)
   - Extract email information and attachments (saved in `output/attachments`)
   - Store the data in separate SQLite databases per PST file at `output/db/`

## Usage

### Command Line Options

```
usage: main.py [-h] [--target-dir TARGET_DIR] [--mbox-dir MBOX_DIR] [--db-path DB_PATH] [--max-workers MAX_WORKERS] [--keep-mbox] [--shared-db]

Convert PST/OST files to MBOX format and extract email data

optional arguments:
  -h, --help            show this help message and exit
  --target-dir TARGET_DIR
                        Directory containing PST/OST files (default: target_files)
  --mbox-dir MBOX_DIR   Directory to store MBOX files (default: mbox_dir)
  --db-path DB_PATH     Path to directory for per-PST databases or a single shared database file (default: output/db)
  --max-workers MAX_WORKERS
                        Maximum number of worker processes for conversion (default: auto)
  --keep-mbox           Keep MBOX files after processing (default: False)
  --shared-db           Use a single shared database for all PST files (default: False)
```

### Directory Structure

- `target_files/`: Place PST/OST files here for processing
- `mbox_dir/`: Temporary directory for MBOX files during conversion
- `output/db/`: 
  - With default settings: Contains separate SQLite database for each PST file (e.g., `output/db/outlook.sqlite3` for `outlook.pst`)
  - With `--shared-db`: Contains a single SQLite database for all PST files (`emaildb.sqlite3`)
- `output/attachments/`: Storage location for extracted email attachments

### Running with Docker

**Using docker-compose (recommended)**:

```bash
# Pull the repository
git clone https://github.com/chrisjohnleah/pst_to_mbox.git
cd pst_to_mbox

# Build and run the container with default settings (separate DB per PST)
docker-compose up
```

**Running with a shared database**:

```bash
docker-compose run --rm app python main.py --shared-db
```

### Running without Docker

```bash
# Clone the repository
git clone https://github.com/chrisjohnleah/pst_to_mbox.git
cd pst_to_mbox

# Install readpst utility (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y pst-utils

# Run the conversion with default settings (separate DB per PST)
python main.py

# For a single shared database
python main.py --shared-db
```

### Database Structure

The tool creates SQLite database(s) with the following schema:

```sql
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
```

By default, a separate database file is created for each PST file, named after the PST file (e.g., `outlook.sqlite3` for `outlook.pst`). This helps to maintain data organisation and makes it easy to know which PST file each email came from.

If you prefer to use a single shared database for all emails, use the `--shared-db` flag.

## Development Mode

For development, we provide a specialised environment that doesn't require rebuilding the Docker image when code changes:

```bash
# Build and run the dev container
make dev

# In a separate terminal, run the tests
make test
```

The development environment includes:
- Hot-reloading (changes to code are immediately available)
- `pytest` for unit testing
- `pytest-cov` for code coverage analysis

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage report
make coverage
```

You can also run the tool directly with Python:

```bash
python3 main.py --target-dir target_files --mbox-dir mbox_dir --db-path output/db
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
