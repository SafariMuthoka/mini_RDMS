# storage/memory.py

class MemoryStorage:
    """
    Simple in-memory storage for table rows.
    Does not enforce schema or constraints.
    """

    def __init__(self):
        self._rows = []

    def insert(self, row: dict):
        self._rows.append(row)

    def all(self):
        return list(self._rows)

    def filter(self, predicate):
        return [row for row in self._rows if predicate(row)]

    def delete(self, predicate):
        remaining = []
        deleted = 0

        for row in self._rows:
            if predicate(row):
                deleted += 1
            else:
                remaining.append(row)

        self._rows = remaining
        return deleted

    def update(self, predicate, updates: dict):
        updated = 0

        for row in self._rows:
            if predicate(row):
                row.update(updates)
                updated += 1

        return updated
