# tidesdb-python

Official Python binding for TidesDB

In active development.. Check back later!


### Usage

```py
# open a database
db = TidesDB.open('/your_db_path')

# create a column family
db.create_column_family("cf_name", 100, 3, 0.5, True, 1, True)

# start a transaction
txn = Transaction.begin(db, "cf_name")

# Put a key-value pair in the transaction
txn.put(b"my_key", b"my_value", 3600)

# commit the transaction
txn.commit()

# get the value back from the database
value = db.get("cf_name", b"my_key")
print(value)

# start a cursor to iterate over key-value pairs
cursor = Cursor.init(db, "cf_name")
cursor.next() # or cursor.prev()
key, value = cursor.get()
print(key, value)

# clean up resources
cursor.free()
db.close()
```
