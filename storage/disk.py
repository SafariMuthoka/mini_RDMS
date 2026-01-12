import json
import os


class DiskStorage:
    def __init__(self, table_name, data_dir="data"):
        self.table_name = table_name
        self.data_dir = data_dir
        self.path = os.path.join(data_dir, f"{table_name}.table")

        os.makedirs(data_dir, exist_ok=True)

    def save(self, schema: dict, rows: list):
        with open(self.path, "w") as f:
            json.dump({
                "schema": schema,
                "rows": rows,
            }, f)

    def load(self):
        if not os.path.exists(self.path):
            return None

        with open(self.path, "r") as f:
            return json.load(f)
