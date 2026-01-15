def require_keys(data, keys):
    for key in keys:
        if key not in data:
            raise ValueError(f"Missing key: {key}")
