import os
import subprocess
import shutil
import logging
from typing import List
from mbox_parser import parse_mbox_file

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

def pst_to_mbox(target_dir: str, mbox_dir: str) -> None:
    """Convert PST/OST files to MBOX format."""
    if not os.path.exists(mbox_dir):
        os.makedirs(mbox_dir)
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith(".pst") or file.endswith(".ost"):
                logging.info(f"Converting {file} to MBOX format...")
                command = ["readpst", "-D", "-b", "-o", os.path.join(mbox_dir), os.path.join(root, file)]
                try:
                    subprocess.run(command, check=True)
                except subprocess.CalledProcessError as e:
                    logging.error(f"Failed to convert {file}: {e}")

def main():
    target_dir = 'target_files'
    mbox_dir = 'mbox_dir'

    # Convert PST/OST files to MBOX
    pst_to_mbox(target_dir, mbox_dir)

    # List and process MBOX files
    mbox_file_list = list_mbox_files(mbox_dir)
    for mbox_file in mbox_file_list:
        logging.info(f"Processing {mbox_file}...")
        try:
            data = parse_mbox_file(mbox_file)
        except Exception as e:
            logging.error(f"Failed to parse {mbox_file}: {e}")

    # Cleanup
    try:
        shutil.rmtree(mbox_dir)
        logging.info(f"Cleaned up temporary directory: {mbox_dir}")
    except Exception as e:
        logging.error(f"Failed to remove directory {mbox_dir}: {e}")

if __name__ == '__main__':
    main()

