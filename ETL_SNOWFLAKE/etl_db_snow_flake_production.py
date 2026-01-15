import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.functions import month, year, col, sum, current_user
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import os
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import databricks.sql

connection_parameters = {
    "account": "KR04362.us-east-2.aws",
    "user": "nguyenton53",
    "password": "Tonuy19530930!",
    "role": "SNOWFLAKE_LEARNING_ROLE",
    "warehouse": "SNOWFLAKE_LEARNING_WH",
    "database": "SNOWFLAKE_LEARNING_DB",
    "schema": "NGUYENTON53_GET_STARTED_WITH_PYTHON"
}

# Create the Snowflake session OUTSIDE main()

session = Session.builder.configs(connection_parameters).create()
print("Connected successfully, session created in snowflake")

# ---------------------------------------------------------
# 2. Define main() — DO NOT create a session inside it
# ---------------------------------------------------------

def main(session):
    print("Running main() with existing session...")

    # Show context
    print("Current context:")

    # -----------------------------------------------------
    # Create tables for python  script to insert data
    # -----------------------------------------------------
    # Create campaign_spend table
    session.sql('''
        CREATE OR REPLACE TABLE campaign_spend (
            CAMPAIGN VARCHAR,
            CHANNEL VARCHAR,
            DATE DATE,
            TOTAL_CLICKS NUMBER,
            TOTAL_COST NUMBER,
            ADS_SERVED NUMBER
        );
    ''').collect()[0][0]

    # Create monthly revenue table
    session.sql('''
        CREATE OR REPLACE TABLE monthly_revenue (
            YEAR NUMBER,
            MONTH NUMBER,
            REVENUE FLOAT
        );
    ''').collect()[0][0]

    # Our data is located in blob storage
    session.sql('''
        CREATE OR REPLACE STAGE blob_stage
        url = 's3://sfquickstarts/ad-spend-roi-snowpark-python-scikit-learn-streamlit/'
        file_format = (
            type = csv
            skip_header = 1
        );
    ''').collect()[0][0]

    # Load the data into our two new tables
    session.sql('''
        COPY INTO campaign_spend
        FROM @blob_stage/campaign_spend;
    ''').collect()[0][0]

    session.sql('''
        COPY INTO monthly_revenue
        FROM @blob_stage/monthly_revenue;
    ''').collect()[0][0]

    session.sql("""
        CREATE OR REPLACE TABLE person (
            NAME VARCHAR,
            HEIGHT FLOAT,
            WEIGHT FLOAT
        );
    """).collect()

    # session.sql("""
    #    SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()
    # """).show()

    # Show tables
    print("Tables in schema:")
    session.sql("SHOW TABLES").show()

    snow_df_spend = session.table('campaign_spend')
    snow_df_revenue = session.table('monthly_revenue')

    ### Total Spend per Year and Month For All Channels
    # Transform the campaign spend data  to total cost per year/month per channel using _group_by()_ and _agg()_ Snowpark DataFrame functions.


    snow_df_spend_per_channel = snow_df_spend.group_by(year('DATE'), month('DATE'),'CHANNEL').agg(sum('TOTAL_COST').as_('TOTAL_COST')).\
    with_column_renamed('"YEAR(DATE)"',"YEAR").with_column_renamed('"MONTH(DATE)"',"MONTH").sort('YEAR','MONTH')

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend per Year and Month For All Channels")
    snow_df_spend_per_channel.show()

    ### Total Spend Across All Channels

    snow_df_spend_per_month = snow_df_spend_per_channel.pivot('CHANNEL',['search_engine','social_media','video','email']).sum('TOTAL_COST').sort('YEAR','MONTH')
    snow_df_spend_per_month = snow_df_spend_per_month.select(
        col("YEAR"),
        col("MONTH"),
        col("'search_engine'").as_("SEARCH_ENGINE"),
        col("'social_media'").as_("SOCIAL_MEDIA"),
        col("'video'").as_("VIDEO"),
        col("'email'").as_("EMAIL")
    )

    # See the output of “print()” and “show()” in the "Output" tab below
    print("Total Spend Across All Channels")
    snow_df_spend_per_month.show()

    ### Total Revenue per Year and Month
    # Use this one for plot chart
    ######################################################
    snow_df_revenue_per_month = (
        snow_df_revenue
        .filter(col("YEAR") == 2013)  # <-- filter here
        .group_by("YEAR", "MONTH")
        .agg(sum("REVENUE"))
        .sort("YEAR", "MONTH")
        .with_column_renamed("SUM(REVENUE)", "REVENUE")
    )

    ######################################################
    # convert to dataframe
    # create subdirectory
    # copy dataframe to  year_month_revenue.csv
    # generate plot char
    # print(type(snow_df_revenue_per_month))  : dataframe

    # Get the current working directory as a Path object
    current_directory = Path.cwd()
    current_directory_str = str(current_directory.absolute())
    print (type(current_directory_str))
    # Print the directory path
    print("Current Working Directory:", current_directory)
    # Define the path using the Path object
    folder_path = "C:/Users/User/PycharmProjects/PythonProject/PROJECT/DEVELOPMENT/ETL_SNOWFLAKE/files"
    filepath =  "C:/Users/User/PycharmProjects/PythonProject/PROJECT/DEVELOPMENT/ETL_SNOWFLAKE/files/revenue_per_month.csv"
    print(folder_path)
    #print(filepath)
    directory_path = Path(folder_path)
    filepath_path  = Path(filepath)

    # filepath_path = Path(filepath)
    # parents=True creates any missing parent directories
    # exist_ok=True prevents an error if the directory already exists

    directory_path.mkdir(parents=True, exist_ok=True)
    print(f"Directory '{directory_path}' ensured to exist.")

    # Save the DataFrame to the CSV file revenue_per_month in the subfolder
    # Convert Snowpark DataFrame → Pandas DataFrame
    pdf = snow_df_revenue_per_month.to_pandas()

    # Save to local CSV
    pdf.to_csv(filepath, index=False, sep=";")

    # generate bar chart

    plt.figure(figsize=(12, 6))
    plt.bar(pdf["MONTH"].astype(str) + "-" + pdf["YEAR"].astype(str), pdf["REVENUE"], color="steelblue")
    plt.title("Revenue per Month")
    plt.xlabel("Month")
    plt.ylabel("Revenue")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    # See the output of “print()” and “show()” in the "Output" tab below
    # print("Total Revenue per Year and Month")
    snow_df_revenue_per_month.show()
    # --- Databricks connection parameters ---
    # server = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    # http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    # access_token = os.environ.get("DATABRICKS_ACCESS_TOKEN")

    server = "dbc-a8cc44ec-b1d1.cloud.databricks.com"
    http_path = "/sql/1.0/warehouses/24ac2a949c389092"""
    access_token = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"""
    catalog = "workspace"
    schema = "default"

    # --- Build Databricks SQLAlchemy URI ---
    engine_uri = (
        f"databricks://token:{access_token}@{server}"
        f"?http_path={http_path}&catalog={catalog}&schema={schema}"
    )

    engine = create_engine(engine_uri)
    df_revenue = snow_df_revenue_per_month.to_pandas()
    # Keep only the 3 columns your Databricks table expects
    df_revenue = df_revenue[["YEAR", "MONTH", "REVENUE"]]
    # Match Databricks column names (lowercase)
    df_revenue.columns = ["year", "month", "revenue"]
    # print(df_revenue.head())
    # print(df_revenue.shape)
    print(df_revenue.columns)

    df_revenue.to_sql(
        name="revenue_yyyymm_ui",
        con=engine,
        if_exists="append",
        index=False
    )

    ### Join Total Spend and Total Revenue per Year and Month Across All Channels
    snow_df_spend_and_revenue_per_month = snow_df_spend_per_month.join(snow_df_revenue_per_month, ["YEAR","MONTH"])

    ### Save Transformed Data into Snowflake Table
    # Transform data into a Snowflake table *SPEND_AND_REVENUE_PER_MONTH*
    snow_df_spend_and_revenue_per_month.write.mode('overwrite').save_as_table('SPEND_AND_REVENUE_PER_MONTH')
    # output of this in "Results" tab below
    return snow_df_spend_and_revenue_per_month


# ---------------------------------------------------------
# 3. Run main() ONCE
# ---------------------------------------------------------

main(session)
