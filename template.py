import os

def create_folder_structure(base_path):
    folders = [
        "data/raw",
        "data/processed",
        "data/external",
        "sql",
        "src/data",
        "src/analysis",
        "src/pipeline",
        "src/utils",
        "tests",
        "notebooks",
        "config",
        "docker"
    ]

    for folder in folders:
        folder_path = os.path.join(base_path, folder)
        os.makedirs(folder_path, exist_ok=True)

    # Create empty files
    files = [
        "src/__init__.py",
        "src/data/__init__.py",
        "src/data/ingest.py",
        "src/data/transform.py",
        "src/data/load.py",
        "src/analysis/__init__.py",
        "src/analysis/analyze.py",
        "src/analysis/visualize.py",
        "src/pipeline/__init__.py",
        "src/pipeline/airflow_dags.py",
        "src/utils/__init__.py",
        "src/utils/database.py",
        "src/utils/logging.py",
        "src/constants/__init__.py",
        "src/main.py",
        "tests/__init__.py",
        "tests/test_ingest.py",
        "tests/test_transform.py",
        "tests/test_analyze.py",
        "notebooks/exploratory_analysis.ipynb",
        "notebooks/visualization.ipynb",
        "config/config.yaml",
        "config/airflow.cfg",
        "docker/Dockerfile",
        "docker/docker-compose.yml",
        "requirements.txt",
        "setup.py",
    ]

    for file in files:
        file_path = os.path.join(base_path, file)
        with open(file_path, 'w') as f:
            pass


if __name__ == "__main__":
    base_path = os.getcwd()
    create_folder_structure(base_path)