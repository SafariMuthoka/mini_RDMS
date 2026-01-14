# sql/executor.py

class SQLExecutionError(Exception):
    pass


class SQLExecutor:
    """
    Executes parsed SQL ASTs against the Database.
    """

    def __init__(self, database):
        self.db = database

    # ================= ENTRY =================

    def execute(self, ast: dict):
        stmt_type = ast["type"]

        if stmt_type == "create_table":
            return self._create_table(ast)
        if stmt_type == "insert":
            return self._insert(ast)
        if stmt_type == "select":
            return self._select(ast)
        if stmt_type == "update":
            return self._update(ast)
        if stmt_type == "delete":
            return self._delete(ast)
        if stmt_type == "show_tables":
            return self._show_tables()

        raise SQLExecutionError(
            f"Unknown SQL statement type '{stmt_type}'"
        )

    # ================= SHOW =================

    def _show_tables(self):
        return self.db.list_tables()

    # ================= CREATE =================

    def _create_table(self, ast):
        self.db.create_table(
            table_name=ast["table"],
            columns=ast["columns"],
            primary_key=ast.get("primary_key"),
            unique_keys=ast.get("unique_keys", []),
            foreign_keys=ast.get("foreign_keys", []),
        )
        return "OK"

    # ================= INSERT =================

    def _insert(self, ast):
        table = self.db.get_table(ast["table"])
        values = ast["values"]

        if "__VALUES__" in values:
            columns = list(table.columns.keys())

            if len(values["__VALUES__"]) != len(columns):
                raise SQLExecutionError(
                    "Column count does not match value count"
                )

            raw_row = dict(zip(columns, values["__VALUES__"]))
        else:
            raw_row = values

        # ✅ TYPE COERCION BASED ON TABLE SCHEMA
        row = {}
        for col, val in raw_row.items():
            expected_type = table.columns[col]

            if val is None:
                row[col] = None
                continue

            try:
                row[col] = expected_type(val)
            except Exception:
                raise SQLExecutionError(
                    f"Column '{col}' expects {expected_type.__name__}"
                )

        self.db.insert(ast["table"], row)
        return "OK"

    # ================= SELECT =================

    def _select(self, ast):
        table = self.db.get_table(ast["table"])

        # 🔥 Indexed WHERE optimization happens inside Table.select
        rows = table.select(ast.get("where"))

        if ast.get("join"):
            rows = self._execute_join(rows, ast)

        fields = ast.get("fields")

        if fields and fields != ["*"]:
            projected = []

            for row in rows:
                out = {}
                for field in fields:
                    col = field.split(".")[-1]
                    if col not in row:
                        raise SQLExecutionError(
                            f"Unknown column '{field}'"
                        )
                    out[col] = row[col]
                projected.append(out)

            rows = projected

        return rows

    # ================= UPDATE =================

    def _update(self, ast):
        def where_fn(row):
            if ast.get("where") is None:
                return True
            return self._eval_where(ast["where"], row)

        return self.db.update(
            ast["table"],
            ast["updates"],
            where_fn
        )

    # ================= DELETE =================

    def _delete(self, ast):
        def where_fn(row):
            if ast.get("where") is None:
                return True
            return self._eval_where(ast["where"], row)

        return self.db.delete(
            ast["table"],
            where_fn
        )

    # ================= WHERE =================

    def _eval_where(self, expr, row):
        op = expr["op"]

        if op == "AND":
            return (
                self._eval_where(expr["left"], row)
                and self._eval_where(expr["right"], row)
            )

        if op == "OR":
            return (
                self._eval_where(expr["left"], row)
                or self._eval_where(expr["right"], row)
            )

        left_col = expr["left"].split(".")[-1]
        right_val = expr["right"]

        if left_col not in row:
            raise SQLExecutionError(
                f"Unknown column '{left_col}'"
            )

        left_val = row[left_col]

        if op == "=":
            return left_val == right_val
        if op == "!=":
            return left_val != right_val
        if op == "<":
            return left_val < right_val
        if op == ">":
            return left_val > right_val

        raise SQLExecutionError(
            f"Unsupported operator '{op}'"
        )

    # ================= JOIN =================

    def _execute_join(self, left_rows, ast):
        join = ast["join"]
        right_table = self.db.get_table(join["table"])
        right_rows = right_table.select()

        left_col, right_col = join["on"]
        left_col = left_col.split(".")[-1]
        right_col = right_col.split(".")[-1]

        joined = []

        for l in left_rows:
            for r in right_rows:
                if l[left_col] == r[right_col]:
                    merged = {}
                    merged.update(l)
                    merged.update(r)
                    joined.append(merged)

        return joined
