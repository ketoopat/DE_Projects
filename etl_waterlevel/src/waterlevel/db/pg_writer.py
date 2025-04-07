import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os

def load_csv_toPostgres(csv_path, table_name='raw.waterlevels'): # to load csv file into a postgresql table using SQL-Alchemy
    POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
    POSTGRES_PW = os.getenv("POSTGRES_PW", "airflow")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "airflow")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    schema_name = table_name.split('.')[0]
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

    # import pdb; pdb.set_trace() # python debugger tool, useful to debug in a docker container.

    
    df = pd.read_csv(csv_path)
    df.to_sql(name=table_name.split('.')[-1], con=engine, schema=table_name.split('.')[0],
              if_exists='append', index=False)
    print(f"✅ Loaded {len(df)} rows into {table_name}")

if __name__ == "__main__":
    load_csv_toPostgres("/opt/airflow/data/raw_waterlevels.csv")
