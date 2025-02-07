from utils.common import get_size
import os
from pathlib import Path
import requests
import zipfile
from utils.logging import logger
from utils.configuration import Configuration

class DataIngestion:
    def __init__(self, config):
        self.config = config

    def download_file(self):
        """Download file from a url and save it at the specified path

        Args:
                url: Source url
                filename: The local filename to save the downloded file as
                download_dir: The local download directory path
        """

        filepath = Path(self.config.data_ingestion.download_dir) / self.config.data_ingestion.filename
            
        if os.path.exists(filepath):
            logger.info(f"{self.config.data_ingestion.filename} of size {get_size(Path(self.config.data_ingestion.download_dir) / self.config.data_ingestion.filename)} already exists!.")     

        else:

            try:
                response = requests.get(self.config.data_ingestion.source_url, stream=True) # stream = True for large files
                response.raise_for_status() # Raise and exception for bad status codes
                with open(filepath, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192): # Chunks for larger files
                        file.write(chunk)
                file.close()
                logger.info(f"Finished downloading {self.config.data_ingestion.filename} at {Path.cwd() / filepath}")
            except Exception as e:
                logger.error(f"Error Downloading file: {e}")
                raise e


    def unzip_file(self):
        """Unzip the files to specified path

        Args:
                filepath: Path to downloaded file
                download_dir: Path to download directory
                unzip_dir: Extract file to this directory
        """
        filepath = Path(self.config.data_ingestion.download_dir) / self.config.data_ingestion.filename
        os.makedirs(self.config.data_ingestion.unzip_dir, exist_ok=True)
        try:
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(self.config.data_ingestion.unzip_dir)
            logger.info(f"Unzipping of {self.config.data_ingestion.filename} to {Path.cwd() / filepath} completed")
        except Exception as e:
            logger.error(f"Unzipping of {self.config.data_ingestion.filename} to {Path.cwd() / filepath} failed")
            raise e


if __name__ == '__main__':

    # Get config
    config = Configuration.get_config()
    
    obj = DataIngestion(config)

    # Download file
    obj.download_file()

    # Unzip file
    obj.unzip_file()

    logger.info("----Data Ingestion Completed----\n")
        