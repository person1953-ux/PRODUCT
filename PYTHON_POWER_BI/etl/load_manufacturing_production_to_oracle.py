import pandas as pd
import oracledb

# Oracle connection (EDIT THESE)
username = "HR"
password = "hr"
dsn = "localhost:1521/XE"  # host:port/service_name

# Create connection
conn = oracledb.connect(
    user=username,
    password=password,
    dsn=dsn
)

# Read CSV
df = pd.read_csv("manufacturing_production.csv")

# Convert timestamp column
df["tnxtimestamp"] = pd.to_datetime(df["tnxtimestamp"])

# Prepare insert
insert_sql = """
INSERT INTO manufacturing_production
(tnxtimestamp, processid, productid, department, lotid, quantity, status)
VALUES (:1, :2, :3, :4, :5, :6, :7)
"""

cursor = conn.cursor()

# Insert data
cursor.executemany(insert_sql, df.values.tolist())
conn.commit()

cursor.close()
conn.close()

print("Data loaded successfully using oracledb!")
