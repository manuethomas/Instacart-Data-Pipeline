from setuptools import setup, find_packages

setup(
    name='instacart_data_pipeline',
    version='0.1.0',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=[
        'pandas',
        'sqlalchemy',
        'psycopg2',
        #'apache-airflow',
        #'docker',
        'pytest',
        'pyyaml',
        'python-box>=5.0,<6.0',
        'requests'
    ],
    entry_points={
        'console_scripts': [
            'run_pipeline=src.main:main',
        ],
    },
)
