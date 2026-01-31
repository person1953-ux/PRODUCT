import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# Database connection details
# -----------------------------
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "test"

# -----------------------------
# CSV file path
# -----------------------------
CSV_FILE_PATH = "manufacturing_production.csv"
                 

# -----------------------------
# Create PostgreSQL connection
# -----------------------------
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# -----------------------------
# Read CSV file
# -----------------------------
df = pd.read_csv(CSV_FILE_PATH)

# -----------------------------
# Load data into PostgreSQL
# -----------------------------
TABLE_NAME = "manufacturing_production"

df.to_sql(
    TABLE_NAME,
    engine,
    if_exists="replace",   # use "append" if table already exists
    index=False
)

print("Data successfully loaded into PostgreSQL!")
