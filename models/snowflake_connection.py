import snowflake.connector
import os
from dotenv import load_dotenv

# Charger les variables d'environnement du fichier .env
load_dotenv()

def connect_to_snowflake():
    snowflake_user = os.getenv("SNOWFLAKE_USER")
    snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
    snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
    snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
    snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    return conn
