import shlex


class SQLParseError(Exception):
    pass


class SQLParser:
    def parse(self, sql: str) -> dict:
        sql = sql.strip().rstrip(";")
        tokens = shlex.split(sql)

        if not tokens:
            raise SQLParseError("Empty SQL statement")

        cmd = tokens[0].upper()

        if cmd == "CREATE":
            return self._parse_create(tokens)
        if cmd == "INSERT":
            return self._parse_insert(sql)
        if cmd == "SELECT":
            return self._parse_select(tokens)
        if cmd == "UPDATE":
            return self._parse_update(tokens)
        if cmd == "DELETE":
            return self._parse_delete(tokens)
        if cmd == "SHOW":
            return self._parse_show(tokens)

        raise SQLParseError(f"Unsupported command '{cmd}'")

    # ================= SHOW =================

    def _parse_show(self, tokens):
        if len(tokens) == 2 and tokens[1].upper() == "TABLES":
            return {"type": "show_tables"}
        raise SQLParseError("Invalid SHOW command")

    # ================= CREATE =================

    def _parse_create(self, tokens):
        if len(tokens) < 4 or tokens[1].upper() != "TABLE":
            raise SQLParseError("Invalid CREATE TABLE syntax")

        table = tokens[2]
        raw = " ".join(tokens[3:])

        if not raw.startswith("(") or not raw.endswith(")"):
            raise SQLParseError("CREATE TABLE requires column definitions")

        inner = raw[1:-1]
        parts = [p.strip() for p in inner.split(",")]

        columns = {}
        primary_key = None
        unique_keys = []
        foreign_keys = []

        for part in parts:
            up = part.upper()

            if up.startswith("PRIMARY KEY"):
                col = part[part.find("(") + 1 : part.find(")")]
                primary_key = col.strip()
                continue

            if up.startswith("UNIQUE"):
                col = part[part.find("(") + 1 : part.find(")")]
                unique_keys.append(col.strip())
                continue

            if up.startswith("FOREIGN KEY"):
                col = part[part.find("(") + 1 : part.find(")")]
                ref = part[up.find("REFERENCES") + 10 :].strip()

                ref_table = ref[: ref.find("(")].strip()
                ref_col = ref[ref.find("(") + 1 : ref.find(")")].strip()

                foreign_keys.append({
                    "column": col.strip(),
                    "ref_table": ref_table,
                    "ref_column": ref_col,
                })
                continue

            pieces = part.split()
            if len(pieces) < 2:
                raise SQLParseError(f"Invalid column definition '{part}'")

            name = pieces[0]
            dtype = self._map_type(pieces[1])
            columns[name] = dtype

            if len(pieces) > 2:
                constraint = pieces[2].upper()
                if constraint == "PRIMARY":
                    primary_key = name
                elif constraint == "UNIQUE":
                    unique_keys.append(name)

        return {
            "type": "create_table",
            "table": table,
            "columns": columns,
            "primary_key": primary_key,
            "unique_keys": unique_keys,
            "foreign_keys": foreign_keys,
        }

    # ================= INSERT =================

    def _parse_insert(self, sql: str):
        upper = sql.upper()

        if "INTO" not in upper or "VALUES" not in upper:
            raise SQLParseError("Invalid INSERT syntax")

        before_vals, values_part = sql.split("VALUES", 1)
        tokens = shlex.split(before_vals)

        table = tokens[2]
        values = self._parse_values(values_part)

        if "(" in before_vals:
            cols_raw = before_vals[
                before_vals.find("(") + 1 : before_vals.find(")")
            ]
            cols = [c.strip() for c in cols_raw.split(",")]

            if len(cols) != len(values):
                raise SQLParseError("Column count does not match value count")

            return {
                "type": "insert",
                "table": table,
                "values": dict(zip(cols, values)),
            }

        return {
            "type": "insert",
            "table": table,
            "values": {"__VALUES__": values},
        }

    # ================= SELECT =================

    def _parse_select(self, tokens):
        i = 1
        fields = []

        while i < len(tokens) and tokens[i].upper() != "FROM":
            fields.append(tokens[i].rstrip(","))
            i += 1

        if i >= len(tokens):
            raise SQLParseError("SELECT missing FROM")

        table = tokens[i + 1]
        i += 2

        if fields == ["*"]:
            fields = None

        join = None
        where = None

        if i < len(tokens) and tokens[i].upper() == "JOIN":
            join_table = tokens[i + 1]

            if tokens[i + 2].upper() != "ON":
                raise SQLParseError("JOIN requires ON")

            left = tokens[i + 3]
            if tokens[i + 4] != "=":
                raise SQLParseError("JOIN condition must use '='")

            right = tokens[i + 5]

            join = {"table": join_table, "on": (left, right)}
            i += 6

        if i < len(tokens) and tokens[i].upper() == "WHERE":
            where = self._parse_where(tokens[i + 1 :])

        return {
            "type": "select",
            "fields": fields,
            "table": table,
            "join": join,
            "where": where,
        }

    # ================= UPDATE =================

    def _parse_update(self, tokens):
        table = tokens[1]

        if tokens[2].upper() != "SET":
            raise SQLParseError("Expected SET")

        updates = {}
        i = 3

        while i < len(tokens) and tokens[i].upper() != "WHERE":
            if tokens[i + 1] != "=":
                raise SQLParseError("Expected '=' in UPDATE")

            updates[tokens[i]] = self._parse_value(tokens[i + 2])

            i += 3
            if i < len(tokens) and tokens[i] == ",":
                i += 1

        where = None
        if i < len(tokens) and tokens[i].upper() == "WHERE":
            where = self._parse_where(tokens[i + 1 :])

        return {
            "type": "update",
            "table": table,
            "updates": updates,
            "where": where,
        }

    # ================= DELETE =================

    def _parse_delete(self, tokens):
        if tokens[1].upper() != "FROM":
            raise SQLParseError("Expected FROM")

        table = tokens[2]
        where = None

        if len(tokens) > 3 and tokens[3].upper() == "WHERE":
            where = self._parse_where(tokens[4:])

        return {
            "type": "delete",
            "table": table,
            "where": where,
        }

    # ================= WHERE =================

    def _parse_where(self, tokens):
        if "AND" in tokens:
            i = tokens.index("AND")
            return {
                "op": "AND",
                "left": self._parse_where(tokens[:i]),
                "right": self._parse_where(tokens[i + 1 :]),
            }

        if "OR" in tokens:
            i = tokens.index("OR")
            return {
                "op": "OR",
                "left": self._parse_where(tokens[:i]),
                "right": self._parse_where(tokens[i + 1 :]),
            }

        if len(tokens) != 3:
            raise SQLParseError("Invalid WHERE clause")

        col, op, val = tokens
        if op != "=":
            raise SQLParseError("Only '=' operator supported")

        return {"op": "=", "left": col, "right": self._parse_value(val)}

    # ================= VALUES =================

    def _parse_values(self, raw: str):
        raw = raw.strip()
        if not raw.startswith("(") or not raw.endswith(")"):
            raise SQLParseError("VALUES must be enclosed in ()")

        inner = raw[1:-1]

        lexer = shlex.shlex(inner, posix=True)
        lexer.whitespace = ","
        lexer.whitespace_split = True

        return [self._parse_value(token.strip()) for token in lexer]

    # ================= UTIL =================

    def _map_type(self, dtype):
        dtype = dtype.upper()
        if dtype in ("INT", "INTEGER"):
            return int
        if dtype in ("TEXT", "STRING"):
            return str
        raise SQLParseError(f"Unknown type '{dtype}'")

    def _parse_value(self, val):
         # shlex already removes quotes
        return val

