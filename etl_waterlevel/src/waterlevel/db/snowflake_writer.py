import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv


def load_csv_to_snowflake_via_copy(csv_path, table_name='RAW.WATERLEVELS'):
    # load_dotenv()  # 🔑 Load variables from .env file
    load_dotenv(dotenv_path="/Users/aiman/DE_Projects/etl_waterlevel/.env")

    # 1. Credentials from env
    SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_PW = os.getenv("SNOWFLAKE_PW")
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

    # 2. Create temp CSV (in case you pass a DataFrame later)
    df = pd.read_csv(csv_path)
    temp_csv = "/tmp/temp_raw_waterlevels.csv"
    df.to_csv(temp_csv, index=False)

    # 3. Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PW,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    # 4. Upload file to user's internal stage (@~)
    print("📤 Uploading CSV to internal stage...")
    put_result = cursor.execute(f"PUT file://{temp_csv} @~/auto_stage OVERWRITE = TRUE").fetchall()
    print(put_result)

    # 5. Run COPY INTO to load data
    print("📥 Running COPY INTO to load data into table...")
    cursor.execute(f"""
        COPY INTO {table_name}
        FROM @~/auto_stage/{os.path.basename(temp_csv)}
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)
    """)
    
    print(f"✅ Loaded data into {table_name}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_csv_to_snowflake_via_copy("/opt/airflow/data/raw_waterlevels.csv")
