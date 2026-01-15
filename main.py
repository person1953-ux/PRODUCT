# This project to use REST API to get data from URL and insert data to mySQL table 
# main.py
from config import DB_CONFIG
from etl.extract import extract_author_data
from etl.transform import normalize_author
from etl.load import load_author

def run():
    # Simulated API response
    data = {
        "authorId": "118985833",
        "url": "semanticscholar.org/author/118985833",
        "papers": [        ],
    }

    author = extract_author_data(data)
    author = normalize_author(author)
    load_author(author, DB_CONFIG)

if __name__ == "__main__":
    run()





