from utils.common import read_yaml
from utils.common import get_size
from constants import CONFIG_FILE_PATH
import os
from pathlib import Path
import requests
import zipfile
from utils.logging import logger

def download_file(url, filename: str, download_dir: str):
    """Download file from a url and save it at the specified path

       Args:
            url: Source url
            filename: The local filename to save the downloded file as
            download_dir: The local download directory path
    """

    filepath = Path(download_dir) / filename
        
    if os.path.exists(filepath):
        logger.info(f"{filename} of size {get_size(Path(download_dir) / filename)} already exists!.")     

    else:

        try:
            response = requests.get(url, stream=True) # stream = True for large files
            response.raise_for_status() # Raise and exception for bad status codes
            with open(filepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192): # Chunks for larger files
                    file.write(chunk)
            file.close()
            logger.info(f"Finished downloading {filename} at {Path.cwd() / filepath}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading file: {e}")
        except OSError:
            logger.error(f"Error writing file: {e}")

def unzip_file(filename, download_dir, unzip_dir):
    """Unzip the files to specified path

       Args:
            filepath: Path to downloaded file
            download_dir: Path to download directory
            unzip_dir: Extract file to this directory
    """
    filepath = Path(download_dir) / filename
    os.makedirs(unzip_dir, exist_ok=True)
    try:
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            zip_ref.extractall(unzip_dir)
        logger.info(f"Unzipping of {filename} to {Path.cwd() / filepath} completed")
    except Exception as e:
        logger.error(f"Unzipping of {filename} to {Path.cwd() / filepath} failed")


if __name__ == "__main__":

    # Get config file
    try:
        config_file = read_yaml(CONFIG_FILE_PATH)   
        logger.info(f"yaml file at {Path.cwd() / CONFIG_FILE_PATH} loaded successfully")
    except Exception as e:
        raise e

    source_url = config_file.data_ingestion.source_url
    download_dir = config_file.data_ingestion.download_dir
    filename = config_file.data_ingestion.filename
    unzip_dir = config_file.data_ingestion.unzip_dir


    # Download file
    download_file(source_url, filename, download_dir)

    # Unzip file
    unzip_file(filename, download_dir, unzip_dir)
        