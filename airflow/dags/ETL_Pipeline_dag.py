from instacart_data_pipeline.utils.common import logger
from instacart_data_pipeline.utils.configuration import Configuration
from instacart_data_pipeline.data.ingest import DataIngestion
from instacart_data_pipeline.data.transform import DataTransformation
from instacart_data_pipeline.data.load import DataLoading
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Create DAG
dag = DAG(
    'ETL_Pipeline',
    default_args=default_args,
    description="Executes SQL files from a directory in PostgreSQL",
    schedule_interval='@daily',
    catchup=False,
)

def extract():
    """Downloads data (a .zip) from a source and unzips it
    """
    # Getting config
    config = Configuration.get_config()

    # STAGE: DataIngestion

    logger.info(">>>>>-----DataIngestion Stage-----<<<<<")
    ingest = DataIngestion(config)
    # Download file
    ingest.download_file()
    # Unzip file
    ingest.unzip_file()
    logger.info("----Data Ingestion Completed----\n")

def transform():
    """Takes in raw datasets, applies the tranformations and saves them in csv format
    """
    # Getting config
    config = Configuration.get_config()

    # STAGE: DataTransformation

    logger.info(">>>>>-----DataTransformation Stage-----<<<<<")
    transform = DataTransformation(config)
    transform.read_files()
    transform.apply_transformation()
    transform.export_as_csv()

    logger.info("----Data Transformation Completed---- \n")

def load():
    """Loads the processed csv files to PostgreSQL Database
    """
    # Getting config
    config = Configuration.get_config()

    # STAGE: DataLoading

    logger.info(">>>>>-----DataLoading Stage-----<<<<<")
    load = DataLoading(config)
    load.load_data()
    logger.info("----Data Loading Completed---- \n")


# Define tasks
extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract,
    dag=dag
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load,
    dag=dag
)

# Set task dependencies
extract_data >> transform_data >> load_data