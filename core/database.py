from typing import Dict
import os

from core.table import Table
from storage.persistence import PersistenceManager


class DatabaseError(Exception):
    pass


class TableAlreadyExistsError(DatabaseError):
    pass


class TableNotFoundError(DatabaseError):
    pass


class Database:
    def __init__(self, name="default_db", data_dir="data"):
        self.name = name
        self._tables: Dict[str, Table] = {}
        self.persistence = PersistenceManager(data_dir)
        self._load_tables()

    # ================= LOAD =================

    def _load_tables(self):
        for filename in self.persistence.list_tables():
            path = os.path.join(self.persistence.data_dir, filename)
            meta, columns = self.persistence.load_table(path)

            table = Table(
                name=meta["name"],
                columns=columns,
                primary_key=meta.get("primary_key"),
                unique_keys=meta.get("unique_keys", []),
                foreign_keys=meta.get("foreign_keys", []),
            )

            self._tables[table.name] = table

        # insert rows AFTER all tables exist (FK safe)
        for filename in self.persistence.list_tables():
            path = os.path.join(self.persistence.data_dir, filename)
            meta, _ = self.persistence.load_table(path)

            table = self._tables[meta["name"]]
            for row in meta.get("rows", []):
                table.insert(row, validate_fk=False)

    # ================= SCHEMA =================

    def create_table(
        self,
        table_name,
        columns,
        primary_key=None,
        unique_keys=None,
        foreign_keys=None,
    ):
        if table_name in self._tables:
            raise TableAlreadyExistsError(
                f"Table '{table_name}' already exists"
            )

        table = Table(
            name=table_name,
            columns=columns,
            primary_key=primary_key,
            unique_keys=unique_keys or [],
            foreign_keys=foreign_keys or [],
        )

        self._tables[table_name] = table
        self.persistence.save_table(table)

    def drop_table(self, table_name):
        if table_name not in self._tables:
            raise TableNotFoundError(
                f"Table '{table_name}' does not exist"
            )

        del self._tables[table_name]

        path = os.path.join(
            self.persistence.data_dir, f"{table_name}.json"
        )
        if os.path.exists(path):
            os.remove(path)

    def list_tables(self):
        return list(self._tables.keys())

    def get_table(self, table_name):
        if table_name not in self._tables:
            raise TableNotFoundError(
                f"Table '{table_name}' does not exist"
            )
        return self._tables[table_name]

    # ================= DATA =================

    def insert(self, table_name, row):
        table = self.get_table(table_name)

        self._check_foreign_keys(table, row)

        table.insert(row)
        self.persistence.save_table(table)

    def update(self, table_name, updates, where):
        table = self.get_table(table_name)

        def wrapped_where(row):
            if where is None:
                return True
            return where(row)

        count = table.update(updates, wrapped_where)

        self.persistence.save_table(table)
        return count

    def delete(self, table_name, where):
        table = self.get_table(table_name)

        def wrapped_where(row):
            if where is None:
                return True
            return where(row)

        count = table.delete(wrapped_where)

        self.persistence.save_table(table)
        return count

    # ================= FOREIGN KEYS =================

    def _check_foreign_keys(self, table: Table, row: dict):
        for fk in table.foreign_keys:
            col = fk["column"]
            ref_table = fk["ref_table"]
            ref_col = fk["ref_column"]

            value = row.get(col)
            if value is None:
                continue

            parent = self.get_table(ref_table)

            exists = any(
                r.get(ref_col) == value for r in parent.rows
            )

            if not exists:
                raise DatabaseError(
                    f"Foreign key violation: "
                    f"{table.name}.{col} references "
                    f"{ref_table}.{ref_col}"
                )
