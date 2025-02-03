from setuptools import setup, find_packages

setup(
    name='instacart-data-pipeline',
    version='0.1',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    install_requires=[
        'pandas',
        'sqlalchemy',
        'psycopg2',
        'apache-airflow',
        'docker',
        'pytest'
    ],
)

