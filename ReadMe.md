Mini SQL Engine with Web App (Python)

A fully in-memory SQL database engine written in Python, featuring a custom SQL parser, executor, indexing system, and an optional Flask web application for executing SQL via HTTP.
This project demonstrates how real database engines work internally — from parsing to execution to storage.

📌 Key Features

SQL Parser → AST → Executor pipeline
In-memory row storage
Table schema validation
PRIMARY KEY & UNIQUE indexes
Fast indexed WHERE column = value
JOIN support(inner join)
Interactive SQL shell (CLI)
Web API (Flask) interface
Zero external database dependencies

📁 Project Structure

project/
│
├── sql/
│   ├── parser.py          # SQL → AST parser
│   ├── executor.py        # AST → execution engine
│
├── storage/
│   └── memory.py          # In-memory storage layer
│
├──core/
     database.py            # Database + table registry
├    table.py               # Table schema, indexes, constraints
├── webapp.py              # Flask web application
├── REPL.py                # CLI SQL REPL
├── README.md
