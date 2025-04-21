FROM apache/airflow:2.7.2-python3.10

# System setup
USER root
RUN apt-get update && \
    apt-get install -y gcc libpq-dev git && \
    rm -rf /var/lib/apt/lists/*

# Python setup
USER airflow
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add our Python code 
COPY src /opt/airflow/src

# After installing requirements and DBT
COPY --chown=airflow:airflow transform /opt/airflow/transform
COPY --chown=airflow:airflow transform/profiles /home/airflow/.dbt

# Create .dbt directory with appropriate permissions
RUN mkdir -p /home/airflow/.dbt && \
    chown -R airflow:root /home/airflow/.dbt

# Environment
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
ENV PATH="/home/airflow/.local/bin:${PATH}"