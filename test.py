from core.table import Table

users = Table(
    name="users",
    columns={"id": int, "name": str},
    primary_key="id",
    unique_keys=["name"]
)

users.insert({"id": 1, "name": "Alice"})
users.insert({"id": 2, "name": "Bob"})

print(users.select())
