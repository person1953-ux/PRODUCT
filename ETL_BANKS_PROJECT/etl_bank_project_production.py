# ---------------------------------------------------------
# ETL Pipeline: World's Largest Banks
# ---------------------------------------------------------

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import mysql.connector


# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
def log_progress(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open("./code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")


# ---------------------------------------------------------
# Extraction
# ---------------------------------------------------------
def extract(url, table_attribs):
    page = requests.get(url, timeout=10).text
    soup = BeautifulSoup(page, 'html.parser')

    df = pd.DataFrame(columns=table_attribs)

    table = soup.find("table", {"class": "wikitable"})
    if table is None:
        raise ValueError("Could not find the expected table on the page")

    rows = table.find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        # Extract bank name safely
        links = cols[1].find_all("a")
        if links:
            bank_name = links[-1].get("title") or links[-1].text.strip()
        else:
            bank_name = cols[1].text.strip()

        # Extract market cap safely
        mc_raw = cols[2].text.strip().replace(",", "").replace("%", "")
        try:
            market_cap = float(mc_raw)
        except ValueError:
            continue

        df = pd.concat([
            df,
            pd.DataFrame({"Name": bank_name, "MC_USD_Billion": market_cap}, index=[0])
        ], ignore_index=True)

    return df


# ---------------------------------------------------------
# Transformation
# ---------------------------------------------------------
def transform(df, csv_path):
    rates = pd.read_csv(csv_path).set_index("Currency")["Rate"].to_dict()

    df["MC_GBP_Billion"] = np.round(df["MC_USD_Billion"] * rates["GBP"], 2)
    df["MC_EUR_Billion"] = np.round(df["MC_USD_Billion"] * rates["EUR"], 2)
    df["MC_INR_Billion"] = np.round(df["MC_USD_Billion"] * rates["INR"], 2)

    return df


# ---------------------------------------------------------
# Load to CSV
# ---------------------------------------------------------
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)


# ---------------------------------------------------------
# Load to SQLite
# ---------------------------------------------------------
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


# ---------------------------------------------------------
# MySQL: Auto-create DB + Table
# ---------------------------------------------------------
def ensure_mysql_schema():
    # Connect WITHOUT database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin"
    )
    cursor = conn.cursor()

    cursor.execute("CREATE DATABASE IF NOT EXISTS banks;")
    conn.commit()

    cursor.close()
    conn.close()

    # Connect WITH database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="banks"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS banks (
            Name VARCHAR(255),
            MC_USD_Billion FLOAT,
            MC_GBP_Billion FLOAT,
            MC_EUR_Billion FLOAT,
            MC_INR_Billion FLOAT
        );
    """)
    conn.commit()

    cursor.close()
    conn.close()


# ---------------------------------------------------------
# Load to MySQL
# ---------------------------------------------------------
def load_to_db_mysql(df):
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="banks"
    )
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO banks (Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion)
        VALUES (%s, %s, %s, %s, %s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()


# ---------------------------------------------------------
# Query Runner
# ---------------------------------------------------------
def run_query(query, sql_connection):
    print(query)
    print(pd.read_sql(query, sql_connection))


# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = "./exchange_rate.csv"
output_path = "./Largest_banks_data.csv"
table_name = "Largest_banks"

log_progress("Starting ETL process")

df = extract(url, table_attribs)
log_progress("Extraction complete")

df = transform(df, csv_path)
log_progress("Transformation complete")

load_to_csv(df, output_path)
log_progress("CSV export complete")

sql_conn = sqlite3.connect("Banks.db")
load_to_db(df, sql_conn, table_name)
log_progress("SQLite load complete")

ensure_mysql_schema()
load_to_db_mysql(df)
log_progress("MySQL load complete")

run_query("SELECT * FROM Largest_banks", sql_conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_conn)
run_query("SELECT Name FROM Largest_banks LIMIT 5", sql_conn)

sql_conn.close()
log_progress("ETL process finished")
