from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

# Create DAG
dag = DAG(
    'Database_setup',
    default_args=default_args,
    description="Executes SQL files from a directory in PostgreSQL",
    schedule_interval=None,
    catchup=False,
)

def execute_sql_files(directory_path, pg_conn_id):
    """Executes all SQL files in a directory against a PostgreSQL database
    """
    pg_hook = PostgresHook(postgres_conn_id = pg_conn_id)

    # Validate directory
    if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
        raise ValueError(f"Directory {directory_path} does not exist or is not a directory")
    
    # Iterate over all sql files in the directory
    sql_files = [f for f in os.listdir(directory_path)]

    if not sql_files:
        return # no sql files exist in the directory
    
    for sql_file in sql_files:
        file_path = os.path.join(directory_path, sql_file)

        try:
            # Read the sql file
            with open(file_path, 'r') as f:
                sql_script = f.read()

            # Run the script
            pg_hook.run(sql_script, autocommit=True)
        except Exception as e:
            raise e


# Define the task
execute_sql = PythonOperator(
    task_id='execute_sql',
    python_callable=execute_sql_files,
    op_kwargs={
        'directory_path': './sql',
        'pg_conn_id': 'postgres_default'
    },
    dag=dag
)