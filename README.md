# PST to MBOX Converter

This project provides a Python script and Docker-based environment for
converting PST (Personal Storage Table) files to MBOX (Mailbox) format. PST
files are commonly used by Microsoft Outlook, while MBOX is a more open and
widely supported mailbox format.

## Features

- Converts PST and OST files to MBOX format.
- Dockerized environment for easy deployment.
- Parses MBOX files and extracts email details, including attachments.
- Stores parsed email data in an SQLite database.
- Suitable for batch processing of PST files.

## Prerequisites

- **Docker**: Ensure you have Docker installed on your system.

## Installation

1. Clone or download this repository to your local machine.

   ```bash
   git clone https://github.com/chrisjohnleah/pst_to_mbox.git

   cd pst_to_mbox

   docker build -t pst_to_mbox .
   ```

````
## Usage

1. Place your PST and OST files that you want to convert into the `target_files` directory.

2. Start a Docker container using the built image:

    ```bash
    docker run -v $(pwd)/target_files:/app/target_files -v $(pwd)/mbox_dir:/app/mbox_dir pst-to-mbox-converter
    ```

    The script will convert the PST and OST files to MBOX format and store them in the `mbox_dir` directory.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- This project utilizes the `readpst` utility for PST file conversion.

## Notes

- Ensure that your PST files are placed in the `target_files` directory before running the Docker container.
- Customize the `main.py` script for any additional processing or parsing requirements.
````
