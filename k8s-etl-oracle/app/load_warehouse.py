############################################################
######################## NEW CODES #########################
import os
import logging
import sys
from typing import List

import pandas as pd
import oracledb


# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------
CSV_PATH = "/data/warehouse.csv"
TABLE_NAME = "WAREHOUSE_DATA"

REQUIRED_ENV_VARS = [
    "ORACLE_HOST",
    "ORACLE_PORT",
    "ORACLE_SERVICE",
    "ORACLE_USER",
    "ORACLE_PASSWORD",
]


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def require_env_vars(names: List[str]) -> dict:
    env = {}
    for name in names:
        value = os.getenv(name)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {name}")
        env[name] = value
    return env


def create_oracle_connection(env: dict) -> oracledb.Connection:
    dsn = oracledb.makedsn(
        host=env["ORACLE_HOST"],
        port=int(env["ORACLE_PORT"]),
        service_name=env["ORACLE_SERVICE"],
    )

    logger.info(
        "Connecting to Oracle: %s:%s/%s",
        env["ORACLE_HOST"],
        env["ORACLE_PORT"],
        env["ORACLE_SERVICE"],
    )

    return oracledb.connect(
        user=env["ORACLE_USER"],
        password=env["ORACLE_PASSWORD"],
        dsn=dsn,
    )


def load_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found at {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("CSV file is empty")

    logger.info(
        "Loaded %d rows and %d columns from CSV",
        len(df),
        len(df.columns),
    )
    return df


def delete_table(
    connection: oracledb.Connection,
    table: str,
) -> None:
    """
    Delete all rows from the target table.
    Safe replacement for TRUNCATE in shared databases.
    """
    logger.info("Deleting data from table %s", table)

    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {table}")
    connection.commit()
    cursor.close()

    logger.info("Table %s cleared using DELETE", table)


def insert_dataframe(
    connection: oracledb.Connection,
    table: str,
    df: pd.DataFrame,
) -> None:
    columns = ",".join(df.columns)
    placeholders = ",".join(f":{i + 1}" for i in range(len(df.columns)))

    table = "WAREHOUSE_DATA"
    columns = "product_id, product_name, quantity, price"
    placeholders = ":1, :2, :3, :4"

    sql = f"""
        INSERT INTO {table} ({columns})
        VALUES ({placeholders})
    """

    cursor = connection.cursor()
    cursor.executemany(sql, df.values.tolist())
    connection.commit()
    cursor.close()

    logger.info("Inserted %d rows into %s", len(df), table)


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main() -> None:
    logger.info("Starting warehouse ETL job")
    env = require_env_vars(REQUIRED_ENV_VARS)
    connection = None

    try:
        connection = create_oracle_connection(env)
        delete_table(connection, TABLE_NAME)
        df = load_csv(CSV_PATH)
        insert_dataframe(connection, TABLE_NAME, df)

        logger.info("ETL job completed successfully")

    except Exception as exc:
        logger.exception("ETL job failed: %s", exc)
        sys.exit(1)

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
