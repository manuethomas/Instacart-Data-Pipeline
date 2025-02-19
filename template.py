import os

def create_folder_structure(base_path):
    folders = [
        "data/raw",
        "data/processed",
        "data/external",
        "sql/analysis",
        "sql/procedures",
        "sql/views",
        "sql/triggers",
        "sql/tables",
        "src/instacart_data_pipeline/data",
        "src/instacart_data_pipeline/constants",
        "src/instacart_data_pipeline/pipeline",
        "src/instacart_data_pipeline/utils",
        "tests",
        "notebooks",
        "config",
        "airflow/config",
        "airflow/dags",
        "airflow/logs",
        "airflow/plugins",
        "airflow/sql",
        "airflow/packages",
        "airflow/data/raw",
        "airflow/data/processed",
        "airflow/data/external"
    ]

    for folder in folders:
        folder_path = os.path.join(base_path, folder)
        os.makedirs(folder_path, exist_ok=True)

    # Create empty files
    files = [
        "src/instacart_data_pipeline/__init__.py",
        "src/instacart_data_pipeline/data/__init__.py",
        "src/instacart_data_pipeline/data/ingest.py",
        "src/instacart_data_pipeline/data/transform.py",
        "src/instacart_data_pipeline/data/load.py",
        "src/instacart_data_pipeline/pipeline/__init__.py",
        "src/instacart_data_pipeline/pipeline/pipeline.py",
        "src/instacart_data_pipeline/utils/__init__.py",
        "src/instacart_data_pipeline/utils/common.py",
        "src/instacart_data_pipeline/utils/configuration.py",
        "src/instacart_data_pipeline/utils/logging.py",
        "src/instacart_data_pipeline/constants/__init__.py",
        "sql/analysis/analysis.sql",
        "sql/procedures/procedures.sql",
        "sql/views/views.sql",
        "sql/triggers/triggers.sql",
        "sql/tables/create_tables.sql"
        "tests/__init__.py",
        "tests/test_ingest.py",
        "tests/test_transform.py",
        "tests/test_analyze.py",
        "notebooks/exploratory_analysis.ipynb",
        "config/config.yaml",
        "airflow/config/instacart_config.yaml",
        "airflow/dags/Databse_Setup_dag.py",
        "airflow/dags/ETL_Pipeline_dag.py",
        "airflow/sql/initial_setup.py"
        "airflow/Dockerfile",
        "airflow/docker-compose.yml",
        "airflow/.dockerignore"
        "requirements.txt",
        "setup.py"
    ]

    for file in files:
        file_path = os.path.join(base_path, file)
        with open(file_path, 'w') as f:
            pass


if __name__ == "__main__":
    base_path = os.getcwd()
    create_folder_structure(base_path)