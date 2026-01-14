# storage/persistence.py

import json
import os


class PersistenceManager:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

    # ================= SAVE =================

    def save_table(self, table):
        path = os.path.join(self.data_dir, f"{table.name}.json")

        data = {
            "name": table.name,
            "columns": {
                col: table.columns[col].__name__
                for col in table.columns
            },
            "primary_key": table.primary_key,
            "unique_keys": table.unique_keys,
            "indexes": list(table._indexes.keys()),
            "foreign_keys": table.foreign_keys,
            "rows": table.rows,
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    # ================= LOAD =================

    def load_table(self, path):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        columns = {
            name: self._map_type(dtype)
            for name, dtype in data["columns"].items()
        }

        return data, columns

    # ================= UTIL =================

    def list_tables(self):
        return [
            f for f in os.listdir(self.data_dir)
            if f.endswith(".json")
        ]

    def _map_type(self, dtype):
        if dtype == "int":
            return int
        if dtype == "str":
            return str
        raise ValueError(f"Unknown type '{dtype}'")
