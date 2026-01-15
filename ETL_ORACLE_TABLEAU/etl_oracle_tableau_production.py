import oracledb
import glob
import pandas as pd
from datetime import datetime

# ------------------ CONFIG ------------------

log_file = "log_file.txt"
target_file = "transformed_data.csv"

# Oracle DB credentials
DB_USER = "HR"
DB_PASSWORD = "hr"
DB_DSN = "localhost:1521/xe"

# Expected schema for ETL + Oracle insert
EXPECTED_COLS = [
    "partnumber", "location", "type", "quantity", "unit",
    "expdate", "parenttype", "classes", "segment", "lotcode",
    "status", "value", "currency", "source", "storedate"
]

# ------------------ LOGGING ------------------

def log_progress(message):
    timestamp = datetime.now().strftime('%Y-%b-%d-%H:%M:%S')
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message}\n")

# ------------------ EXTRACT ------------------

def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)

    # Normalize column names (critical)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace('\ufeff', '', regex=True)
    )

    # Drop unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # Drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    return df


def extract():
    extracted_data = pd.DataFrame()

    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            df = extract_from_csv(csvfile)
            extracted_data = pd.concat([extracted_data, df], ignore_index=True)

    return extracted_data

# ------------------ TRANSFORM ------------------

def transform(df):
    # Remove unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # Remove duplicate columns
    df = df.loc[:, ~df.columns.duplicated()]

    # Fix typos
    df = df.rename(columns={
        "parentyype": "parenttype",
        "class": "classes"
    })

    # Add missing expected columns
    for col in EXPECTED_COLS:
        if col not in df.columns:
            df[col] = None

    # Drop unexpected columns and enforce correct order
    df = df[EXPECTED_COLS]

    return df

# ------------------ LOAD ------------------

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# ------------------ ETL PROCESS ------------------

log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")

# ------------------ ORACLE INSERT ------------------

# Convert NaN → None for Oracle
df = transformed_data.astype(object).where(pd.notna(transformed_data), None)

# Convert DataFrame rows → list of tuples
data_to_insert = [tuple(row) for row in df.values]

sql = """
INSERT INTO HR.INVENTORY_LOT  (
    partnumber, location, type, quantity, unit,
    expdate, parenttype, classes, segment, lotcode,
    status, value, currency, source, storedate
) VALUES (
    :1, :2, :3, :4, :5,
    :6, :7, :8, :9, :10,
    :11, :12, :13, :14, :15
)
"""

try:
    with oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN) as connection:
        print("Connected to Oracle Database")

        with connection.cursor() as cursor:
            cursor.executemany(sql, data_to_insert)
            connection.commit()

            print("Data successfully inserted into inventorylot")

except oracledb.Error as error:
    print(f"Error connecting to ORACLE DB: {error}")
