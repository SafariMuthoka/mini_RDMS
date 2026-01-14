## MiniRDMS Engine (Python)
A lightweight, Relational Database Management System engine implemented in pure Python.
The project demonstrates SQL parsing, query execution, schema enforcement, indexing, and persistence, without relying on external database systems.

This engine supports a meaningful subset of SQL while remaining simple, readable, and extensible.

## Project Objectives
Demonstrate how SQL engines work internally
Implement parsing → execution → storage flow
Enforce schema constraints (PK, UNIQUE, FK)
Support indexed queries for performance
Persist data using JSON storage
Serve as a learning and assessment project,.

## Features
✅ Data Definition (DDL)
CREATE TABLE
Column type enforcement (INT, TEXT)
PRIMARY KEY
UNIQUE
FOREIGN KEY

✅ Data Manipulation (DML)
INSERT INTO table VALUES (...)
INSERT INTO table (columns...) VALUES (...)
SELECT *
SELECT column1, column2

## WHERE conditions:
=, !=, <, >
AND, OR
UPDATE ... SET ... WHERE ...
DELETE FROM ... WHERE ...

✅ Indexing
## Automatic indexes on:
Primary keys
Unique columns
Indexed equality lookups for fast SELECTs

✅ JOIN Support
## Inner joins using:
SELECT ...
FROM table1
JOIN table2 ON table1.col = table2.col;

✅ #Storage
## In-memory execution
Persistent JSON storage (/data directory)
Tables reload automatically on restart

## Archtecture Overview
.RDMS
├── core/
│   ├── table.py
│   └── database.py
├── sql/
│   ├── parser.py
│   └── executor.py
├── storage/
│   ├── memory.py
│   └── persistence.py
├── data/
│   └── .gitkeep
├── web/
│   └── app.py   (or index.html / backend)
├── repl.py
├── README.md
└── requirements.txt (optional)
