rdbms/
│
├── core/
│   ├── database.py        # Database orchestration
│   ├── table.py           # Table abstraction
│   ├── row.py             # Row validation & typing
│   ├── index.py           # Indexing logic
│
├── sql/
│   ├── parser.py          # SQL parsing
│   ├── executor.py        # SQL → engine mapping
│
├── storage/
│   ├── memory.py          # In-memory storage
│   └── disk.py            # (future) persistence
│
├── api/
│   ├── db_service.py      # Service layer (used by web/app)
│
├── repl/
│   └── shell.py           # Interactive REPL
│
├── web/
│   └── app.py             # Flask demo app
│
├── tests/
│   └── test_db.py
│
└── main.py


