# repl.py


from core.database import Database
from sql.parser import SQLParser, SQLParseError
from sql.executor import SQLExecutor, SQLExecutionError


def print_result(result):
    if result is None:
        return

    if isinstance(result, str):
        print(result)
        return

    if isinstance(result, int):
        print(f"{result} row(s) affected")
        return

    if isinstance(result, list):
        if not result:
            print("(empty result)")
            return

        # SELECT results (list of dicts)
        if isinstance(result[0], dict):
            headers = list(result[0].keys())
            print(" | ".join(headers))
            print("-" * (len(headers) * 10))
            for row in result:
                print(" | ".join(str(row[h]) for h in headers))
            return

        # SHOW TABLES (list of strings)
        for item in result:
            print(item)
        return

    print(result)


def repl():
    db = Database()
    parser = SQLParser()
    executor = SQLExecutor(db)

    print("RDBMS Interactive Shell")
    print("Type SQL statements ending with ';'")
    print("Type 'exit' or 'quit' to leave\n")

    buffer = ""

    while True:
        try:
            prompt = "db> " if not buffer else "... "
            line = input(prompt).strip()

            if line.lower() in ("exit", "quit"):
                print("Bye 👋")
                break

            buffer += " " + line

            if not buffer.strip().endswith(";"):
                continue

            sql = buffer.strip()
            buffer = ""

            # Strip SQL comments
            sql = sql.split("--")[0].strip()
            if not sql:
                continue

            ast = parser.parse(sql)
            result = executor.execute(ast)
            print_result(result)

        except (SQLParseError, SQLExecutionError) as e:
            print(f"Error: {e}")
            buffer = ""

        except KeyboardInterrupt:
            print("\nInterrupted (buffer cleared)")
            buffer = ""

        except Exception as e:
            print(f"Unexpected error: {e}")
            buffer = ""


if __name__ == "__main__":
    repl()

