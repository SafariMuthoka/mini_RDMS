from flask import Flask, request, render_template
from core.database import Database
from sql.parser import SQLParser, SQLParseError
from sql.executor import SQLExecutor, SQLExecutionError

app = Flask(__name__)

# Shared RDBMS components (same engine as REPL)
db = Database()
parser = SQLParser()
executor = SQLExecutor(db)


@app.route("/", methods=["GET", "POST"])
def index():
    """
    Web interface for executing SQL statements against the custom RDBMS.
    Supports CREATE, INSERT, SELECT, UPDATE, DELETE, SHOW TABLES.
    """
    result = None
    error = None
    sql = ""

    if request.method == "POST":
        sql = request.form.get("sql", "").strip()

        if sql:
            try:
                ast = parser.parse(sql)
                result = executor.execute(ast)
            except (SQLParseError, SQLExecutionError) as e:
                error = str(e)
            except Exception as e:
                error = f"Unexpected error: {e}"

    return render_template(
        "index.html",
        sql=sql,
        result=result,
        error=error
    )


if __name__ == "__main__":
    app.run(debug=True)
