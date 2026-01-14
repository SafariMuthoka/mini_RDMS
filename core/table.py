from typing import Dict, List, Callable, Any
from storage.memory import MemoryStorage


class TableError(Exception):
    pass


class SchemaError(TableError):
    pass


class ConstraintViolationError(TableError):
    pass


class Table:
    def __init__(
        self,
        name: str,
        columns: Dict[str, type],
        primary_key: str = None,
        unique_keys: List[str] = None,
        foreign_keys: List[dict] = None,
        indexes: List[str] = None,
    ):
        self.name = name
        self.columns = columns
        self.primary_key = primary_key
        self.unique_keys = unique_keys or []
        self.foreign_keys = foreign_keys or []
        self.indexes = indexes or []

        self._storage = MemoryStorage()

        # column -> { value -> [rows] }
        self._indexes: Dict[str, Dict[Any, List[dict]]] = {}

        self._validate_schema()
        self._init_indexes()

    # ================= SCHEMA =================

    def _validate_schema(self):
        if self.primary_key and self.primary_key not in self.columns:
            raise SchemaError(
                f"Primary key '{self.primary_key}' not in schema"
            )

        for key in self.unique_keys:
            if key not in self.columns:
                raise SchemaError(
                    f"Unique key '{key}' not in schema"
                )

        for key in self.indexes:
            if key not in self.columns:
                raise SchemaError(
                    f"Index '{key}' not in schema"
                )

        for fk in self.foreign_keys:
            if fk["column"] not in self.columns:
                raise SchemaError(
                    f"Foreign key column '{fk['column']}' not in schema"
                )

    def _init_indexes(self):
        all_indexes = set(self.indexes)
        if self.primary_key:
            all_indexes.add(self.primary_key)
        all_indexes.update(self.unique_keys)

        for col in all_indexes:
            self._indexes[col] = {}

    # ================= INTERNAL =================

    def _validate_row(self, row: Dict):
        for col in row:
            if col not in self.columns:
                raise SchemaError(f"Unknown column '{col}'")

        for col, col_type in self.columns.items():
            val = row.get(col)
            if val is not None and not isinstance(val, col_type):
                raise SchemaError(
                    f"Column '{col}' expects {col_type.__name__}"
                )

    def _check_constraints(self, row: Dict, ignore_row=None):
        for key in [self.primary_key] + self.unique_keys:
            if not key:
                continue

            value = row.get(key)
            if value is None:
                continue

            bucket = self._indexes.get(key, {}).get(value, [])
            for existing in bucket:
                if existing is not ignore_row:
                    raise ConstraintViolationError(
                        f"Duplicate value '{value}' for key '{key}'"
                    )

    def _add_indexes(self, row: Dict):
        for col, index in self._indexes.items():
            value = row.get(col)
            if value is None:
                continue
            index.setdefault(value, []).append(row)

    def _remove_indexes(self, row: Dict):
        for col, index in self._indexes.items():
            value = row.get(col)
            if value in index:
                index[value].remove(row)
                if not index[value]:
                    del index[value]

    # ================= CRUD =================

    def insert(self, row: Dict, validate_fk: bool = True):
        self._validate_row(row)

        full_row = {col: row.get(col) for col in self.columns}

        self._check_constraints(full_row)

        self._storage.insert(full_row)
        self._add_indexes(full_row)

    def select(self, where=None):
        if where is None:
            return self._storage.all()

        # indexed equality lookup
        if (
            isinstance(where, dict)
            and where.get("op") == "="
            and where.get("left") in self._indexes
        ):
            value = where["right"]
            return list(self._indexes[where["left"]].get(value, []))

        if callable(where):
            return self._storage.filter(where)

        raise TableError("Unsupported WHERE condition")

    def update(self, updates: Dict, where: Callable):
        updated = 0

        for row in list(self._storage.all()):
            if not where(row):
                continue

            new_row = row.copy()
            new_row.update(updates)

            self._validate_row(new_row)
            self._check_constraints(new_row, ignore_row=row)

            self._remove_indexes(row)
            row.update(updates)
            self._add_indexes(row)

            updated += 1

        return updated

    def delete(self, where: Callable):
        to_delete = [row for row in self._storage.all() if where(row)]

        for row in to_delete:
            self._remove_indexes(row)

        return self._storage.delete(where)

    # ================= ACCESS =================

    @property
    def rows(self):
        return self._storage.all()
