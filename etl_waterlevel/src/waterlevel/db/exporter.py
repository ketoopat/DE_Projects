import pandas as pd
from sqlalchemy import create_engine


def export_table_to_csv(output_path, table="raw.waterlevels"):
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )
    schema, table_name = table.split(".")
    df = pd.read_sql_table(table_name, con=engine, schema=schema)
    df.to_csv(output_path, index=False)
    print(f"✅ Exported {len(df)} rows to {output_path}")

if __name__ == "__main__":
    export_table_to_csv("/opt/airflow/data/exported_waterlevels.csv")