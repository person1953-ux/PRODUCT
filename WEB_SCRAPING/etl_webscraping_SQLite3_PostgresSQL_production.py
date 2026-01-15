import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
import csv
import io
from sqlalchemy import create_engine

######################## DB INSERT ######################

def insert_csv_with_pandas(csv_file_path, table_name, db_connection_string):
    """
    Inserts data from a CSV file into a PostgreSQL table using pandas.
    """
    try:
        # Read the CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)
        if 'Unnamed: 0' in df.columns:
            df = df.drop(columns=['Unnamed: 0'])
        # Create an engine to connect to the database
        engine = create_engine(db_connection_string)

        # Write the DataFrame to the SQL table
        # if_exists='append' adds to an existing table
        # index=False prevents writing the DataFrame index as a column
        df.to_sql(table_name, engine, index=False, if_exists='append')

        print(f"Data imported successfully into table '{table_name}' using pandas.")

    except Exception as e:
        print(f"Error: {e}")


#########################  SCRAPING CODES ##################
url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
#csv_path = '/home/project/top_50_films.csv'  # home = "/Users/user/"
#csv_path = '/home/PycharmProjects/PythonProject/PROJECT/DEVELOPMENT/WEB_SCRAPING/top_50_films.csv'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
count = 0
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')
tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

for row in rows:
    if count < 50:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

print(df)
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()
df.to_csv(csv_path)

################ INSERT DATA TO POSTGRESQL #######################
### FROM CSV ####
# Example usage
db_connection_string = 'postgresql://postgres:admin@localhost:5432/test'
csv_file_path = 'top_50_films.csv'
table_name = 'BEST_FILM_TEST'
#insert_csv_to_postgres(csv_file_path, table_name, db_params)
insert_csv_with_pandas(csv_file_path, table_name, db_connection_string)
