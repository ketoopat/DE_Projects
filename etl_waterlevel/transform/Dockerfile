FROM python:3.11-slim 
# Lightweight base Python image

# Install dbt core + adapters
RUN pip install --upgrade pip \
  && pip install dbt-core==1.9.0 dbt-postgres==1.9.0 dbt-snowflake==1.9.0

WORKDIR /usr/app
COPY . .

# Optional if you're using dbt_packages.yml
RUN dbt deps

ENV DBT_USER=prod_user
