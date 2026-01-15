# ============================
#          IMPORTS
# ============================

import glob
import numpy as np
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# Oracle
import oracledb

# MySQL
import mysql.connector

# Snowflake
from snowflake.snowpark import Session


# ============================
#          CONFIG
# ============================

LOG_FILE = "log_file.txt"
TARGET_FILE = "transformed_data.csv"


# ============================
#       EXTRACT FUNCTIONS
# ============================

def extract_from_csv(file_path):
    return pd.read_csv(file_path)


def extract_from_json(file_path):
    return pd.read_json(file_path)


def extract_from_xml(file_path):
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_path)
    root = tree.getroot()

    for person in root:
        df = pd.concat([
            df,
            pd.DataFrame([{
                "name": person.find("name").text,
                "height": float(person.find("height").text),
                "weight": float(person.find("weight").text)
            }])
        ], ignore_index=True)

    return df


def extract():
    extracted = pd.DataFrame(columns=["name", "height", "weight"])

    # CSV
    for csvfile in glob.glob("*.csv"):
        if csvfile != TARGET_FILE:
            extracted = pd.concat([extracted, extract_from_csv(csvfile)], ignore_index=True)

    # JSON
    for jsonfile in glob.glob("*.json"):
        extracted = pd.concat([extracted, extract_from_json(jsonfile)], ignore_index=True)

    # XML
    for xmlfile in glob.glob("*.xml"):
        extracted = pd.concat([extracted, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted


# ============================
#       TRANSFORM
# ============================

def transform(df):
    df["height"] = round(df["height"] * 0.0254, 2)   # inches → meters
    df["weight"] = round(df["weight"] * 0.45359237, 2)  # pounds → kg
    return df


# ============================
#       LOAD TO CSV
# ============================

def load_to_csv(file_path, df):
    df.to_csv(file_path, index=False)


# ============================
#       LOGGING
# ============================

def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%b-%d-%H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp}, {message}\n")


# ============================
#       ORACLE LOADER
# ============================

def load_to_oracle(df):
    DB_USER = "HR"
    DB_PASSWORD = "hr"
    DB_DSN = "localhost:1521/xe"

    # Convert NaN → None for Oracle
    df_oracle = df.astype(object).where(pd.notna(df), None)

    rows = [tuple(row) for row in df_oracle[["name", "height", "weight"]].values]

    try:
        with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as conn:
            with conn.cursor() as cursor:
                sql = "INSERT INTO PERSON (NAME, HEIGHT, WEIGHT) VALUES (:1, :2, :3)"
                cursor.executemany(sql, rows)
                conn.commit()
                print(f"Inserted {len(rows)} rows into Oracle PERSONS table.")
    except Exception as e:
        print("Oracle insert error:", e)


# ============================
#       MYSQL LOADER
# ============================

def load_to_mysql(df):
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="employees"
        )
        cursor = conn.cursor()

        sql = "INSERT INTO public.person (name, height, weight) VALUES (%s, %s, %s)"

        for _, row in df.iterrows():
            cursor.execute(sql, (row["name"], row["height"], row["weight"]))

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Inserted {len(df)} rows into MySQL persons table.")
    except Exception as e:
        print("MySQL insert error:", e)


# ============================
#       SNOWFLAKE LOADER
# ============================

def load_to_snowflake(df):
    try:
        # Create a clean copy for Snowflake
        df_sf = df.copy()


        # Convert numeric columns back to float
        df_sf["height"] = pd.to_numeric(df_sf["height"], errors="coerce")
        df_sf["weight"] = pd.to_numeric(df_sf["weight"], errors="coerce")

        # Debug print to confirm dtype
        print("Snowflake dtypes:\n", df_sf.dtypes)

        connection_parameters = {
            "account": "KR04362.us-east-2.aws",
            "user": "nguyenton53",
            "password": "Tonuy19530930!",
            "role": "SNOWFLAKE_LEARNING_ROLE",
            "warehouse": "SNOWFLAKE_LEARNING_WH",
            "database": "SNOWFLAKE_LEARNING_DB",
            "schema": "NGUYENTON53_GET_STARTED_WITH_PYTHON"
        }

        session = Session.builder.configs(connection_parameters).create()

        df_sf = df[["name", "height", "weight"]]
        df_sp = session.create_dataframe(df_sf)
        df_sp.write.save_as_table("PERSON_PY", mode="append")

        print(f"Inserted {len(df_sf)} rows into Snowflake PERSON_PY.")
    except Exception as e:
        print("Snowflake insert error:", e)


# ============================
#       MAIN ETL PIPELINE
# ============================

log_progress("ETL Job Started")

log_progress("Extract phase Started")
data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
data = transform(data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_to_csv(TARGET_FILE, data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")

# Load into databases
load_to_oracle(data)
load_to_mysql(data)
load_to_snowflake(data)
