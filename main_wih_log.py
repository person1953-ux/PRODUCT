# main.py
import logging
from config import DB_CONFIG
from etl.extract import extract_author_data
from etl.transform import normalize_author
from etl.load import load_author

# -----------------------------
# Logging Configuration
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("etl_pipeline")

def run():
    logger.info("Starting ETL pipeline for author data")

    try:
        # Simulated API response
        data = {
            "authorId": "118985833",
            "url": "semanticscholar.org/author/118985833",
            "papers": []
        }

        logger.info("Extracting data")
        author = extract_author_data(data)

        logger.info("Transforming data")
        author = normalize_author(author)

        logger.info("Loading data into database")
        load_author(author, DB_CONFIG)

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.exception("ETL pipeline failed due to an error")
        raise

if __name__ == "__main__":
    run()
