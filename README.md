# tidesdb-python

Official Python bindings for TidesDB v1.

TidesDB is a fast and efficient key-value storage engine library written in C. The underlying data structure is based on a log-structured merge-tree (LSM-tree). This Python binding provides a Pythonic interface to TidesDB with full support for all v1 features.

## Features

- **ACID Transactions** - Atomic, consistent, isolated, and durable transactions across column families
- **Optimized Concurrency** - Multiple concurrent readers, writers don't block readers
- **Column Families** - Isolated key-value stores with independent configuration
- **Bidirectional Iterators** - Iterate forward and backward over sorted key-value pairs
- **TTL Support** - Time-to-live for automatic key expiration
- **Compression** - Snappy, LZ4, or ZSTD compression support
- **Bloom Filters** - Reduce disk reads with configurable false positive rates
- **Background Compaction** - Automatic or manual SSTable compaction with parallel execution
- **Sync Modes** - Three durability levels: NONE, BACKGROUND, FULL
- **Custom Comparators** - Register custom key comparison functions
- **Error Handling** - Detailed error codes for production use
- **Context Managers** - Pythonic resource management with `with` statements
- **Python Iterator Protocol** - Use iterators in for loops and comprehensions

## Installation

### Prerequisites

You must have the TidesDB v1 shared C library installed on your system.

**Building TidesDB:**
```bash
# Clone TidesDB repository
git clone https://github.com/tidesdb/tidesdb.git
cd tidesdb

# Build and install (compile with sanitizer and tests OFF for bindings)
rm -rf build && cmake -S . -B build -DTIDESDB_WITH_SANITIZER=OFF -DTIDESDB_BUILD_TESTS=OFF
cmake --build build
sudo cmake --install build
```

**Dependencies:**
- Snappy
- LZ4
- Zstandard
- OpenSSL

**On Ubuntu/Debian:**
```bash
sudo apt install libzstd-dev liblz4-dev libsnappy-dev libssl-dev
```

**On macOS:**
```bash
brew install zstd lz4 snappy openssl
```

### Install Python Package

```bash
pip install tidesdb
```

Or install from source:
```bash
git clone https://github.com/tidesdb/tidesdb-python.git
cd tidesdb-python
pip install -e .
```

## Quick Start

```python
from tidesdb import TidesDB, ColumnFamilyConfig

# Open database (with optional parameters)
with TidesDB("./mydb", enable_debug_logging=False, max_open_file_handles=1024) as db:
    # Create column family
    db.create_column_family("users")
    
    # Write data
    with db.begin_txn() as txn:
        txn.put("users", b"user:1", b"Alice")
        txn.put("users", b"user:2", b"Bob")
        txn.commit()
    
    # Read data
    with db.begin_read_txn() as txn:
        value = txn.get("users", b"user:1")
        print(f"Value: {value.decode()}")  # Output: Value: Alice
```

## Usage

### Opening and Closing a Database

```python
from tidesdb import TidesDB

# Using context manager (recommended)
with TidesDB("./mydb") as db:
    # Use database
    pass

# Manual open/close with options
db = TidesDB(
    "./mydb",
    enable_debug_logging=False,
    max_open_file_handles=1024  # 0 = unlimited, >0 = cache up to N open files
)
# Use database
db.close()
```

### Creating and Dropping Column Families

```python
from tidesdb import ColumnFamilyConfig, CompressionAlgo, SyncMode

# Create with default configuration
db.create_column_family("my_cf")

# Create with custom configuration
config = ColumnFamilyConfig(
    memtable_flush_size=128 * 1024 * 1024,  # 128MB
    max_sstables_before_compaction=128,      # Trigger compaction at 128 SSTables
    compaction_threads=4,                    # Use 4 threads for parallel compaction
    max_level=12,
    probability=0.25,
    compressed=True,
    compress_algo=CompressionAlgo.LZ4,
    bloom_filter_fp_rate=0.01,              # 1% false positive rate
    enable_background_compaction=True,
    background_compaction_interval=1000000,  # Check every 1 second (microseconds)
    use_sbha=True,
    sync_mode=SyncMode.BACKGROUND,
    sync_interval=1000                       # Sync every 1 second (milliseconds)
)

db.create_column_family("my_cf", config)

# Drop a column family
db.drop_column_family("my_cf")
```

### CRUD Operations

All operations are performed through transactions for ACID guarantees.

#### Writing Data

```python
# Simple write
with db.begin_txn() as txn:
    txn.put("my_cf", b"key", b"value")
    txn.commit()

# Multiple operations
with db.begin_txn() as txn:
    txn.put("my_cf", b"key1", b"value1")
    txn.put("my_cf", b"key2", b"value2")
    txn.put("my_cf", b"key3", b"value3")
    txn.commit()
```

#### Writing with TTL

```python
import time

with db.begin_txn() as txn:
    # Expire in 10 seconds
    ttl = int(time.time()) + 10
    txn.put("my_cf", b"temp_key", b"temp_value", ttl)
    txn.commit()

# TTL examples
ttl = -1                              # No expiration
ttl = int(time.time()) + 300          # Expire in 5 minutes
ttl = int(time.time()) + 3600         # Expire in 1 hour
```

#### Reading Data

```python
with db.begin_read_txn() as txn:
    value = txn.get("my_cf", b"key")
    print(f"Value: {value.decode()}")
```

#### Deleting Data

```python
with db.begin_txn() as txn:
    txn.delete("my_cf", b"key")
    txn.commit()
```

#### Transaction Rollback

```python
# Manual rollback
with db.begin_txn() as txn:
    txn.put("my_cf", b"key", b"value")
    txn.rollback()  # Changes not applied

# Automatic rollback on exception
try:
    with db.begin_txn() as txn:
        txn.put("my_cf", b"key", b"value")
        raise ValueError("Error!")
except ValueError:
    pass  # Transaction automatically rolled back
```

### Iterating Over Data

```python
# Forward iteration
with db.begin_read_txn() as txn:
    with txn.new_iterator("my_cf") as it:
        it.seek_to_first()
        
        while it.valid():
            key = it.key()
            value = it.value()
            print(f"Key: {key}, Value: {value}")
            it.next()

# Backward iteration
with db.begin_read_txn() as txn:
    with txn.new_iterator("my_cf") as it:
        it.seek_to_last()
        
        while it.valid():
            key = it.key()
            value = it.value()
            print(f"Key: {key}, Value: {value}")
            it.prev()

# Using Python iterator protocol
with db.begin_read_txn() as txn:
    with txn.new_iterator("my_cf") as it:
        it.seek_to_first()
        
        for key, value in it:
            print(f"Key: {key}, Value: {value}")

# Get all items as list
with db.begin_read_txn() as txn:
    with txn.new_iterator("my_cf") as it:
        it.seek_to_first()
        items = list(it)  # List of (key, value) tuples
```

### Column Family Statistics

```python
stats = db.get_column_family_stats("my_cf")

print(f"Column Family: {stats.name}")
print(f"Comparator: {stats.comparator_name}")
print(f"Number of SSTables: {stats.num_sstables}")
print(f"Total SSTable Size: {stats.total_sstable_size} bytes")
print(f"Memtable Size: {stats.memtable_size} bytes")
print(f"Memtable Entries: {stats.memtable_entries}")
print(f"Compression: {stats.config.compressed}")
print(f"Bloom Filter FP Rate: {stats.config.bloom_filter_fp_rate}")
print(f"Sync Mode: {stats.config.sync_mode}")
```

### Listing Column Families

```python
cf_list = db.list_column_families()
print(f"Column families: {cf_list}")
```

### Compaction

```python
# Automatic background compaction (set during CF creation)
config = ColumnFamilyConfig(
    enable_background_compaction=True,
    max_sstables_before_compaction=512,  # Trigger at 512 SSTables
    compaction_threads=4                  # Use 4 threads
)
db.create_column_family("my_cf", config)

# Manual compaction (requires minimum 2 SSTables)
cf = db.get_column_family("my_cf")
cf.compact()
```

### Sync Modes

```python
from tidesdb import SyncMode

# TDB_SYNC_NONE - Fastest, least durable (OS handles flushing)
config = ColumnFamilyConfig(sync_mode=SyncMode.NONE)

# TDB_SYNC_BACKGROUND - Balanced (fsync every N milliseconds)
config = ColumnFamilyConfig(
    sync_mode=SyncMode.BACKGROUND,
    sync_interval=1000  # Sync every 1 second
)

# TDB_SYNC_FULL - Most durable (fsync on every write)
config = ColumnFamilyConfig(sync_mode=SyncMode.FULL)
```

### Compression Algorithms

```python
from tidesdb import CompressionAlgo

# No compression (set compressed=False)
config = ColumnFamilyConfig(
    compressed=False
)

# Snappy (fast, default)
config = ColumnFamilyConfig(
    compressed=True,
    compress_algo=CompressionAlgo.SNAPPY  # Value: 0
)

# LZ4 (very fast)
config = ColumnFamilyConfig(
    compressed=True,
    compress_algo=CompressionAlgo.LZ4  # Value: 1
)

# Zstandard (high compression)
config = ColumnFamilyConfig(
    compressed=True,
    compress_algo=CompressionAlgo.ZSTD  # Value: 2
)
```

## Working with Python Objects

### Using Pickle

```python
import pickle

# Store Python objects
user_data = {
    "name": "John Doe",
    "age": 30,
    "email": "john@example.com"
}

with db.begin_txn() as txn:
    key = b"user:123"
    value = pickle.dumps(user_data)
    txn.put("users", key, value)
    txn.commit()

# Retrieve Python objects
with db.begin_read_txn() as txn:
    key = b"user:123"
    value = txn.get("users", key)
    user_data = pickle.loads(value)
    print(user_data)
```

### Using JSON

```python
import json

# Store JSON
data = {"name": "Alice", "score": 95}

with db.begin_txn() as txn:
    key = b"player:1"
    value = json.dumps(data).encode()
    txn.put("players", key, value)
    txn.commit()

# Retrieve JSON
with db.begin_read_txn() as txn:
    key = b"player:1"
    value = txn.get("players", key)
    data = json.loads(value.decode())
    print(data)
```

## Error Handling

```python
from tidesdb import TidesDBException, ErrorCode

try:
    with db.begin_read_txn() as txn:
        value = txn.get("my_cf", b"nonexistent_key")
except TidesDBException as e:
    print(f"Error: {e}")
    print(f"Error code: {e.code}")
    
    if e.code == ErrorCode.TDB_ERR_NOT_FOUND:
        print("Key not found")
    elif e.code == ErrorCode.TDB_ERR_MEMORY:
        print("Out of memory")
    # ... handle other errors
```

**Error Codes:**
- `TDB_SUCCESS` (0) - Operation successful
- `TDB_ERR_MEMORY` (-2) - Memory allocation failed
- `TDB_ERR_INVALID_ARGS` (-3) - Invalid arguments
- `TDB_ERR_IO` (-4) - I/O error
- `TDB_ERR_NOT_FOUND` (-5) - Key not found
- `TDB_ERR_EXISTS` (-6) - Resource already exists
- `TDB_ERR_CORRUPT` (-7) - Data corruption
- `TDB_ERR_LOCK` (-8) - Lock acquisition failed
- `TDB_ERR_TXN_COMMITTED` (-9) - Transaction already committed
- `TDB_ERR_TXN_ABORTED` (-10) - Transaction aborted
- `TDB_ERR_READONLY` (-11) - Write on read-only transaction
- `TDB_ERR_FULL` (-12) - Database full
- `TDB_ERR_INVALID_NAME` (-13) - Invalid name
- `TDB_ERR_INVALID_CF` (-16) - Invalid column family
- `TDB_ERR_THREAD` (-17) - Thread operation failed
- `TDB_ERR_CHECKSUM` (-18) - Checksum verification failed
- `TDB_ERR_KEY_DELETED` (-19) - Key is deleted (tombstone)
- `TDB_ERR_KEY_EXPIRED` (-20) - Key has expired (TTL)

## Complete Example

```python
from tidesdb import TidesDB, ColumnFamilyConfig, CompressionAlgo, SyncMode
import time
import json

# Open database
with TidesDB("./example_db") as db:
    # Create column family with custom configuration
    config = ColumnFamilyConfig(
        memtable_flush_size=64 * 1024 * 1024,
        compressed=True,
        compress_algo=CompressionAlgo.LZ4,
        bloom_filter_fp_rate=0.01,
        enable_background_compaction=True,
        sync_mode=SyncMode.BACKGROUND,
        sync_interval=1000
    )
    
    db.create_column_family("users", config)
    
    # Write data
    users = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]
    
    with db.begin_txn() as txn:
        for user in users:
            key = f"user:{user['id']}".encode()
            value = json.dumps(user).encode()
            txn.put("users", key, value)
        
        # Add temporary session data with TTL
        session_key = b"session:abc123"
        session_value = b"session_data"
        ttl = int(time.time()) + 3600  # Expire in 1 hour
        txn.put("users", session_key, session_value, ttl)
        
        txn.commit()
    
    # Read data
    with db.begin_read_txn() as txn:
        value = txn.get("users", b"user:1")
        user = json.loads(value.decode())
        print(f"User: {user}")
    
    # Iterate over all users
    print("\nAll users:")
    with db.begin_read_txn() as txn:
        with txn.new_iterator("users") as it:
            it.seek_to_first()
            for key, value in it:
                if key.startswith(b"user:"):
                    user = json.loads(value.decode())
                    print(f"  {user['name']} - {user['email']}")
    
    # Get statistics
    stats = db.get_column_family_stats("users")
    print(f"\nDatabase Statistics:")
    print(f"  Memtable Size: {stats.memtable_size} bytes")
    print(f"  Memtable Entries: {stats.memtable_entries}")
    print(f"  Number of SSTables: {stats.num_sstables}")
    
    # Clean up
    db.drop_column_family("users")
```

## Performance Tips

1. **Batch operations** in transactions for better performance
2. **Use appropriate sync mode** for your durability requirements
3. **Enable background compaction** for automatic maintenance
4. **Adjust memtable flush size** based on your workload
5. **Use compression** to reduce disk usage and I/O
6. **Configure bloom filters** to reduce unnecessary disk reads
7. **Set appropriate TTL** to automatically expire old data
8. **Use parallel compaction** for faster SSTable merging
9. **Use context managers** to ensure proper resource cleanup

## Testing

```bash
# Run tests
python -m pytest test_tidesdb.py -v

# Run with coverage
python -m pytest test_tidesdb.py --cov=tidesdb --cov-report=html
```

## Type Hints

The package includes full type hints for better IDE support:

```python
from tidesdb import TidesDB, ColumnFamilyConfig, Transaction
from typing import Optional

def get_user(db: TidesDB, user_id: int) -> Optional[bytes]:
    with db.begin_read_txn() as txn:
        try:
            key = f"user:{user_id}".encode()
            return txn.get("users", key)
        except TidesDBException:
            return None
```

## Concurrency

TidesDB is designed for high concurrency:

- **Multiple readers can read concurrently** - No blocking between readers
- **Writers don't block readers** - Readers can access data during writes
- **Writers block other writers** - Only one writer per column family at a time
- **Read transactions** (`begin_read_txn`) acquire read locks
- **Write transactions** (`begin_txn`) acquire write locks on commit
- **Different column families** can be written concurrently

## License

Multiple licenses apply:

```
Mozilla Public License Version 2.0 (TidesDB)

-- AND --

BSD 3 Clause (Snappy)
BSD 2 (LZ4)
BSD 2 (xxHash - Yann Collet)
BSD (Zstandard)
Apache 2.0 (OpenSSL 3.0+) / OpenSSL License (OpenSSL 1.x)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For issues, questions, or discussions:
- GitHub Issues: https://github.com/tidesdb/tidesdb-python/issues
- Discord Community: https://discord.gg/tWEmjR66cy
- Main TidesDB Repository: https://github.com/tidesdb/tidesdb

## Links

- [TidesDB Main Repository](https://github.com/tidesdb/tidesdb)
- [TidesDB Documentation](https://github.com/tidesdb/tidesdb#readme)
- [Other Language Bindings](https://github.com/tidesdb/tidesdb#bindings)
- [PyPI Package](https://pypi.org/project/tidesdb/)