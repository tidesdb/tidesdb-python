# tidesdb-python

Official Python binding for TidesDB

In active development.. Check back later!


### Usage

```
 # Open the database
db = TidesDB.open('/path/to/tidesdb')

# Create a column family
db.create_column_family("cf_name", 100, 3, 0.5, True, 1, True)

# Start a transaction
txn = Transaction.begin(db, "cf_name")

# Put a key-value pair in the transaction
txn.put(b"my_key", b"my_value", 3600)

# Commit the transaction
txn.commit()

# Get the value back from the database
value = db.get("cf_name", b"my_key")
print(value)

# Start a cursor to iterate over key-value pairs
cursor = Cursor.init(db, "cf_name")
cursor.next() # or cursor.prev()
key, value = cursor.get()
print(key, value)

# Clean up resources
cursor.free()
db.close()
```
