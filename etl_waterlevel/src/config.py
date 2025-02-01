# src/config.py

DB_USERNAME = "postgres"
# DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "etl_waterlevel"

DATABASE_URL = f"postgresql://{DB_USERNAME}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
