FROM apache/airflow:2.10.5

# Copy and Install package
COPY ./packages /tmp/packages
RUN pip install --no-cache-dir /tmp/packages/instacart_data_pipeline-0.1.0.tar.gz

# Set back to default airflow workdir
WORKDIR /opt/airflow

