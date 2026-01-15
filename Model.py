# Model.py
import mysql.connector  # or your DB driver of choice

class Model:
    def __init__(self, table_name, data, **db_config):
        self.table_name = table_name
        self.data = data
        self.db_config = db_config

    def _get_connection(self):
        return mysql.connector.connect(**self.db_config)

    def save(self):
        if not self.data:
            return

        columns = list(self.data.keys())
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        ### INSERT INTO TABLE AUTHORS
        query = f"INSERT INTO {self.table_name} ({col_list}) VALUES ({placeholders})"

        values = [self.data[col] for col in columns]

        conn = self._get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(query, values)
            conn.commit()
        finally:
            conn.close()




