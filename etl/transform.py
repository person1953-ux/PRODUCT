# etl/transform.py

def normalize_author(author):
    """
    Normalize/clean author fields.
    """
    if not author:
        return author

    normalized = author.copy()

    url = normalized.get("url")
    if url and not url.startswith("https://"):
        normalized["url"] = "https://" + url

    return normalized
