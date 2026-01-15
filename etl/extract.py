# etl/extract.py

def extract_author_data(raw):
    """
    Extract only the fields we care about from the raw API response.
    """
    return {
        "author_id": raw.get("authorId"),
        "url": raw.get("url"),
        "papers_count": len(raw.get("papers", [])),
    }
