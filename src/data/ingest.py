import pandas as pd
import os
from utils.database import get_db_connection
from utils.logging import logger

def ingest_data(data_source):
    """Ingests data from CSV files in a directory into a PostgreSQL database."""

    # Establish database connection
    engine = get_db_connection()
    if engine is None:
        logger.error("Failed to establish databae connection. Exiting..")
        return

    # Define input directory
    input_dir = os.path.join(os.getcwd(),'data', data_source)

    if not os.path.exists(input_dir):
        logger.error(f"Input directory not found: {input_dir}")
        return

    if not os.listdir(input_dir):
        logger.error("Input directory is empty")
        return
    
    csv_files_flag = False

    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            csv_files_flag = True
            input_path = os.path.join(input_dir, filename)
            table_name = filename[:-4]

            logger.info(f"Found {filename} in {input_dir}. Starting upload to table '{table_name}'..." )

            try:
                # Read CSV in chunks and load data into PostgreSQL
                for chunk in pd.read_csv(input_path, chunksize=500000):

                    try:
                        chunk.to_sql(table_name, engine, if_exists='append', index=False)
                        logger.info(f"{len(chunk)} rows processed and added to {table_name}.")
                    except Exception as e:
                        logger.error(f"Error loading chunk into '{table_name}': {e}")

            except pd.errors.EmptyDataError:
                logger.error(f"Error: CSV file is empty: {input_path}")
            except pd.errors.ParserError:
                logger.error(f"Error parsing csv file: {input_path}")
            except Exception as e:
                logger.error(f"Error reading csv file {input_path}: {e}")

            logger.info(f"Finished uploading {filename} to {table_name}.")

    if not csv_files_flag:
        logger.error("No CSV files found")

    logger.info("Data ingestion completed without error")

if __name__ == '__main__':
    data_source = 'raw' # Can be raw / processed / external
    ingest_data(data_source)

