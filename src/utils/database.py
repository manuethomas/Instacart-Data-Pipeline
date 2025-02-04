import yaml
from sqlalchemy import create_engine, text
from utils.logging import logger


def get_db_connection():
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