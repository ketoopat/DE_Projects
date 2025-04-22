import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def load_csv_to_snowflake_via_copy(csv_path, table_name='RAW.WATERLEVELS'):
    load_dotenv(dotenv_path="/opt/airflow/.env")

    # 1. Credentials from env
    SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
    SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
    SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE")
    
    # 2. Load and convert your PEM key into DER bytes
    private_key_path = "/opt/airflow/rsa_key.p8"
    with open(private_key_path, 'rb') as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )

    private_key_der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    # 3. Create temp CSV (in case you pass a DataFrame later)
    df = pd.read_csv(csv_path)
    temp_csv = "/tmp/temp_raw_waterlevels.csv"
    df.to_csv(temp_csv, index=False)

    # 4. Connect to Snowflake using key pair auth
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        private_key=private_key_der
    )
    # import pdb; pdb.set_trace()
    cursor = conn.cursor()

    # 5. Upload file to user's internal stage (@~)
    print("📤 Uploading CSV to internal stage...")
    put_result = cursor.execute(
        f"PUT file://{temp_csv} @~/auto_stage OVERWRITE = TRUE"
    ).fetchall()
    print(put_result)

    # 6. Run COPY INTO to load data
    print("📥 Running COPY INTO to load data into table...")
    cursor.execute(f"""
        COPY INTO {table_name}
        FROM @~/auto_stage/{os.path.basename(temp_csv)}
        FILE_FORMAT = (
            TYPE = 'CSV' 
            FIELD_OPTIONALLY_ENCLOSED_BY = '\"' 
            SKIP_HEADER = 1
        )
    """)
    
    print(f"✅ Loaded data into {table_name}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_csv_to_snowflake_via_copy("/opt/airflow/data/raw_waterlevels.csv")
