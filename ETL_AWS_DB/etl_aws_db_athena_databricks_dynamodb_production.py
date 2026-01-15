import os
from pathlib import Path
import json
from decimal import Decimal
import boto3
import awswrangler as wr
import pandas as pd
from databricks import sql
import logging

# =========================================================
# LOGGING CONFIGURATION
# =========================================================
logging.basicConfig(
    filename="etl_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


# =========================================================
# CONFIG
# =========================================================
AWS_REGION = "us-east-2"
S3_BUCKET = "nhan-s3-bucket"
S3_PREFIX = "data"
LOCAL_CSV_PATH = r"C:/Users/User/PycharmProjects/PythonProject/PROJECT/DEVELOPMENT/ETL_AWS_DB/revenue_per_month.csv"
ATHENA_DB = "database-2-athena"
ATHENA_TABLE = "revenue_per_month"

DATABRICKS_SERVER_HOSTNAME = os.getenv(
    "DATABRICKS_SERVER_HOSTNAME",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)
DATABRICKS_HTTP_PATH = os.getenv(
    "DATABRICKS_HTTP_PATH",
    "/sql/1.0/warehouses/24ac2a949c389092"
)
DATABRICKS_ACCESS_TOKEN = os.getenv(
    "DATABRICKS_ACCESS_TOKEN",
    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)


# =========================================================
# DYNAMODB HELPERS
# =========================================================
def convert_to_decimal(item):
    for key, value in item.items():
        if isinstance(value, (float, int)):
            item[key] = Decimal(str(value))
    return item


def get_dynamodb_resource():
    return boto3.resource("dynamodb", region_name=AWS_REGION)
def get_dynamodb_client():
    return boto3.client("dynamodb", region_name=AWS_REGION)
def load_csv_to_dynamodb(csv_file_path, table_name):
    logger.info("Loading CSV into DynamoDB...")

    full_path = (
        "C:/Users/User/PycharmProjects/PythonProject/PROJECT/DEVELOPMENT/ETL_AWS_DB/"
        + csv_file_path
    )

    dynamodb = get_dynamodb_resource()
    client = get_dynamodb_client()
    table = dynamodb.Table(table_name)

    resp = client.describe_table(TableName=table_name)
    logger.info(f"DynamoDB KeySchema: {resp['Table']['KeySchema']}")
    logger.info(f"DynamoDB AttributeDefinitions: {resp['Table']['AttributeDefinitions']}")

    df = pd.read_csv(full_path)
    df.columns = df.columns.str.strip().str.lower()

    if not {"year", "month"}.issubset(df.columns):
        logger.error("CSV missing required columns")
        raise ValueError("CSV must contain 'year' and 'month' columns")

    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)

    items = json.loads(df.to_json(orient="records"))

    try:
        with table.batch_writer() as batch:
            for item in items:
                item = convert_to_decimal(item)
                item["YYYY"] = str(item.pop("year"))
                item["MM"] = str(item.pop("month"))
                batch.put_item(Item=item)

        logger.info(f"Loaded {len(items)} items into DynamoDB table '{table_name}'")

    except Exception as e:
        logger.error(f"DynamoDB error: {e}")


# =========================================================
# S3 UPLOAD
# =========================================================
def upload_csv_to_s3(local_path: str, bucket: str, prefix: str) -> str:
    session = boto3.session.Session(region_name=AWS_REGION)
    s3_client = session.client("s3")

    file_name = Path(local_path).name
    s3_key = f"{prefix}/{file_name}"

    try:
        s3_client.upload_file(local_path, bucket, s3_key)
        logger.info(f"Uploaded {local_path} -> s3://{bucket}/{s3_key}")
    except Exception as e:
        raise RuntimeError(f"S3 upload error: {e}")

    s3_folder = f"s3://{bucket}/{Path(s3_key).parent}/"
    logger.info(f"Athena folder: {s3_folder}")
    return s3_folder

# =========================================================
# ATHENA / GLUE
# =========================================================
def ensure_athena_db_exists(db_name: str):
    wr.config.region = AWS_REGION
    try:
        wr.catalog.create_database(name=db_name)
        logger.info(f"Created Athena DB: {db_name}")
    except Exception:
        logger.info(f"Athena DB '{db_name}' already exists")

def create_athena_table_from_csv(s3_folder: str, db_name: str, table_name: str):
    wr.config.region = AWS_REGION

    columns_types = {
        "year": "int",
        "month": "int",
        "revenue": "double"
    }

    try:
        wr.catalog.create_csv_table(
            database=db_name,
            table=table_name,
            path=s3_folder,
            sep=";",  # your CSV uses semicolons
            skip_header_line_count=1,
            columns_types=columns_types
        )
        logger.info(f"Created Athena table '{table_name}'")
    except Exception as e:
        raise RuntimeError(f"Athena table creation error: {e}")


def query_athena_table(db_name: str, table_name: str) -> pd.DataFrame:
    wr.config.region = AWS_REGION

    sql_query = f"""
    SELECT *
    FROM "{db_name}"."{table_name}"
    ORDER BY year, month
    LIMIT 10;
    """

    logger.info(f"Athena query:\n{sql_query}")
    df = wr.athena.read_sql_query(sql=sql_query, database=db_name)
    logger.info(f"Athena returned {len(df)} rows")
    return df


# =========================================================
# DATABRICKS EXPORT
# =========================================================
def export_to_databricks(df: pd.DataFrame):
    table_name = "revenue_yyyymm_ui"

    df = df.dropna(subset=["year", "month", "revenue"])

    conn = sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_ACCESS_TOKEN,
    )

    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        year INT,
        month INT,
        revenue DOUBLE
    )
    """)

    for _, row in df.iterrows():
        cursor.execute(
            f"INSERT INTO {table_name} (year, month, revenue) VALUES (?, ?, ?)",
            (int(row["year"]), int(row["month"]), float(row["revenue"]))
        )

    cursor.close()
    conn.close()
    logger.info(f"Inserted {len(df)} rows into Databricks table '{table_name}'")


# =========================================================
# MAIN
# =========================================================
def main():
    logger.info("=== ETL Pipeline Started ===")

    s3_folder = upload_csv_to_s3(
        local_path=LOCAL_CSV_PATH,
        bucket=S3_BUCKET,
        prefix=S3_PREFIX,
    )

    ensure_athena_db_exists(ATHENA_DB)

    create_athena_table_from_csv(
        s3_folder=s3_folder,
        db_name=ATHENA_DB,
        table_name=ATHENA_TABLE,
    )

    df = query_athena_table(
        db_name=ATHENA_DB,
        table_name=ATHENA_TABLE,
    )

    export_to_databricks(df)

    load_csv_to_dynamodb("revenue_per_month.csv", "revenue_per_month")

    logger.info("=== ETL Pipeline Completed Successfully ===")


if __name__ == "__main__":
    main()

