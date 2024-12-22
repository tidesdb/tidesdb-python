# tidesdb-python

Official Python binding for TidesDB

### Usage

```py
# open a database.  
# will reopen one if existing
db = TidesDB.open('your_db_dir')

# create a column family
db.create_column_family("cf_name", 1024*1024*64, 12, 0.24, True, COMPRESS_SNAPPY, True, SKIP_LIST)

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
