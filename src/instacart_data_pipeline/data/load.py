import os
from instacart_data_pipeline.utils.configuration import Configuration
from instacart_data_pipeline.utils.logging import logger
import pandas as pd
from pathlib import Path
from instacart_data_pipeline.utils.common import get_db_connection

class DataLoading:
    def __init__(self, config):
        self.config = config
        self.source_dir = self.config.data_loading.source_dir

    def load_data(self):
        """Loads the processed csv files to PostgreSQL Database"""
        try:
            # Establish Database connection
            engine = get_db_connection()

            # Use context manager to handle transaction and connection
            with engine.begin() as connection:

                try:
                    # Get csv files
                    for filename in os.listdir(self.source_dir):
                        if filename.endswith(".csv"):
                            filepath = Path(self.source_dir) / filename
                            tablename = filename[:-4]

                            logger.info(f"Found {filename} in {self.source_dir}. Starting upload to table '{tablename}'")

                    # Start uploading in chunks

                        for chunk in pd.read_csv(filepath, chunksize=500000):
                            chunk.to_sql(tablename, connection, if_exists='append', index=False)
                            logger.info(f"{len(chunk)} rows processed and added to {tablename}")
                    

                except Exception as e:
                    logger.error(f"Error uploading '{filename}' to '{tablename}': {e}")
                    raise e
            
        except Exception as e:
            logger.error(f"Error loading csv files to PostgreSQL database: {e}")
            raise e
    


if __name__ == '__main__':
    # Get config
    config = Configuration.get_config()
    
    obj = DataLoading(config)
    obj.load_data()

    logger.info("----Data Loading Completed----\n")