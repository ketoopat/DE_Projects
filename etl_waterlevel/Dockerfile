FROM apache/airflow:2.7.2-python3.10

# System setup
USER root
RUN apt-get update && \
    apt-get install -y gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Python setup
USER airflow
WORKDIR /opt/airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Environment
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"