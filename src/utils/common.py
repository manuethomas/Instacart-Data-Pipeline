import yaml
from sqlalchemy import create_engine, Engine, text
from utils.logging import logger
from pathlib import Path
import os
from box import ConfigBox
from box.exceptions import BoxValueError
import pandas as pd

def get_db_connection() -> Engine:
    """Establish PostgreSQL Database Connection

       Returns:
            An Engine object which is a pool of connections
    """

    # Load the configuration file
    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Extract database credentials from the configuration file
    db_config = config['database']
    host = db_config['host']
    port = db_config['port']
    user = db_config['user']
    password = db_config['password']
    dbname = db_config['dbname']

    # Create the database connection string
    connection_string = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'

    # Create and return the database engine
    engine = create_engine(connection_string)
    return engine

def execute_sql_file(file_path, engine):

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            with open(file_path, 'r') as file:
                sql_script = file.read()
                # Split the script into individual statements
                statements = sql_script.split(';')
                for statement in statements:
                    if statement.strip():  # Skip empty statements
                        #logger.info(f"Executing statement: {statement.strip()}")
                        connection.execute(text(statement))
                        logger.info("Statement executed successfully")
            transaction.commit()
            logger.info("Transaction committed successfully")
        except Exception as e:
            transaction.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")

def read_yaml(filepath: Path) -> ConfigBox:
    """Reads a yaml file and returns its contents as a python dictionary

       Args:
            filepath: Path to the file
        
       Returns:
            The contents of the file as a dictionary
    """
    try:
        with open(filepath, 'r') as file:
            yaml_content = yaml.safe_load(file)
            return ConfigBox(yaml_content)
    except BoxValueError:
        raise ValueError("yaml file is empty")
    except Exception as e:
        raise e
    

def get_size(filepath: Path) -> str:
    """Takes a filepath and returns the filesize

       Args:
            filepath: Path to file
        
       Returns:
            The size of a file in Mega Bytes
    """
    size_on_disk = os.path.getsize(filepath)
    if(size_on_disk < 1024**1024):
        return f"~ {round(size_on_disk/(1024), 2)} KB"
    else:
        return f"~ {round(size_on_disk/(1024*1024), 2)} MB"
    

def query_database_to_dataframe(query: str) -> pd.DataFrame:
    """Queries a PostgreSQL database and returns a pandas dataframe

       Args:
            query: The SQL query to execute

       Returns:
            A pandas dataframe containing the query results
    
    """
    try:
        # Create database engine
        engine = get_db_connection()

        # Execute the query and read the result into a Dataframe
        df = pd.read_sql_query(query, engine)

        # Close connection
        engine.dispose()

        return df
    except Exception as e:
        logger.error(f"Failed reading SQL query {query}: \n{e}")
        raise e