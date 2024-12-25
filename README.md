# tidesdb-python

Official Python binding for TidesDB

### Usage

#### Basic operations
```py
from tidesdb import TidesDB, TidesDBCompressionAlgo, TidesDBMemtableDS

# Open a TidesDB database
db = TidesDB.open('my_db')

# Create a column family
db.create_column_family(
    "my_column_family", 
    1024*1024*64,    # Flush threshold (64MB)
    12,              # Max level skip list, if using hash table is irrelevant
    0.24,            # Probability skip list, if using hash table is irrelevant
    True,            # Enable compression
    TidesDBCompressionAlgo.COMPRESS_SNAPPY,  # Compression algorithm can be NO_COMPRESSION, COMPRESS_SNAPPY, COMPRESS_LZ4, COMPRESS_ZSTD
    True,            # Enable bloom filter
    TidesDBMemtableDS.SKIP_LIST  # Use skip list for memtable
)

# Put key-value pair into the database
db.put("my_column_family", b"key1", b"value1", ttl=3600)

# Get the value for the key
value = db.get("my_column_family", b"key1")
print(f"Key: key1, Value: {value}")

# Delete the key-value pair
db.delete("my_column_family", b"key1")

# Try to get the deleted key (should raise an exception or return None)
try:
    value = db.get("my_column_family", b"key1")
except Exception as e:
    print(f"Error: {e}")

# Close the database
db.close()

```


#### Using Transactions
```py
from tidesdb import TidesDB, Transaction, TidesDBCompressionAlgo, TidesDBMemtableDS

# Open the database
db = TidesDB.open('my_db')

# Create a column family
db.create_column_family(
    "my_column_family", 
    1024*1024*64,    # Flush threshold (64MB)
    12,              # Max level skip list, if using hash table is irrelevant
    0.24,            # Probability skip list, if using hash table is irrelevant
    True,            # Enable compression
    TidesDBCompressionAlgo.COMPRESS_SNAPPY,  # Compression algorithm can be NO_COMPRESSION, COMPRESS_SNAPPY, COMPRESS_LZ4, COMPRESS_ZSTD
    True,            # Enable bloom filter
    TidesDBMemtableDS.SKIP_LIST  # Use skip list for memtable
)

# Begin a transaction on the column family
txn = Transaction.begin(db, "my_column_family")

# Put multiple key-value pairs in the transaction
txn.put(b"key2", b"value2", ttl=3600)
txn.put(b"key3", b"value3", ttl=3600)

# Commit the transaction
txn.commit()

# Verify the values
value2 = db.get("my_column_family", b"key2")
value3 = db.get("my_column_family", b"key3")
print(f"Key: key2, Value: {value2}")
print(f"Key: key3, Value: {value3}")

# Close the database
db.close()

```

#### Using Cursor
```py
from tidesdb import TidesDB, Cursor, TidesDBCompressionAlgo, TidesDBMemtableDS

# Open the database
db = TidesDB.open('my_db')

# Create a column family
db.create_column_family(
    "my_column_family", 
    1024*1024*64,   # Flush threshold (64MB)
    12,              # Max level skip list, if using hash table is irrelevant
    0.24,            # Probability skip list, if using hash table is irrelevan
    True,            # Enable compression
    TidesDBCompressionAlgo.COMPRESS_SNAPPY,  # Compression algorithm can be NO_COMPRESSION, COMPRESS_SNAPPY, COMPRESS_LZ4, COMPRESS_ZSTD
    True,            # Enable bloom filter
    TidesDBMemtableDS.SKIP_LIST  # Use skip list for memtable or HASH_TABLE
)

# Insert some key-value pairs
db.put("my_column_family", b"key1", b"value1", ttl=3600)
db.put("my_column_family", b"key2", b"value2", ttl=3600)
db.put("my_column_family", b"key3", b"value3", ttl=3600)

# Initialize the cursor to iterate over the column family
cursor = Cursor.init(db, "my_column_family")

# Loop to fetch and print all key-value pairs
try:
    while True:
        key, value = cursor.get()
        print(f"Key: {key}, Value: {value}")
        cursor.next()  # Move to the next element
except Exception as e:
    # Exception is raised when there are no more elements to iterate
    print("End of column family reached.")

try:
    while True:
        key, value = cursor.get()
        print(f"Key: {key}, Value: {value}")
        cursor.prev()  # Move to the previous element
except Exception as e:
    # Exception is raised when there are no more elements to iterate
    print("Start of column family reached.")

# Free the cursor resources
cursor.free()

# Close the database
db.close()

```

#### Listing Column Families
```py
from tidesdb import TidesDB

# Open the database
db = TidesDB.open('my_db')

# List all column families in the database
column_families = db.list_column_families()
print(f"Column families: \n{column_families}")

# Close the database
db.close()

```


#### Dropping Column Family
```py
from tidesdb import TidesDB

# Open the database
db = TidesDB.open('my_db')

# Drop the specified column family
db.drop_column_family("my_column_family")

# Close the database
db.close()

```