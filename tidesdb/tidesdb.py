"""
TidesDB Python Bindings v1

Copyright (C) TidesDB
Original Author: Alex Gaetano Padula

Licensed under the Mozilla Public License, v. 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.mozilla.org/en-US/MPL/2.0/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import ctypes
from ctypes import c_void_p, c_char_p, c_int, c_size_t, c_int64, c_float, c_double, POINTER, Structure
from typing import Optional, List, Tuple, Dict
from enum import IntEnum
import os


# Load the TidesDB shared library
def _load_library():
    """Load the TidesDB shared library."""
    lib_names = ['libtidesdb.so', 'libtidesdb.dylib', 'tidesdb.dll']
    
    for lib_name in lib_names:
        try:
            return ctypes.CDLL(lib_name)
        except OSError:
            continue
    
    # Try system paths
    try:
        return ctypes.CDLL('tidesdb')
    except OSError:
        raise RuntimeError(
            "Could not load TidesDB library. "
            "Please ensure libtidesdb is installed and in your library path."
        )


_lib = _load_library()


def _get_libc():
    """Get the C standard library for memory management operations."""
    import sys
    from ctypes.util import find_library
    
    if sys.platform == 'win32':
        return ctypes.cdll.msvcrt
    else:
        # Use find_library to locate the correct libc
        libc_name = find_library('c')
        if libc_name:
            return ctypes.CDLL(libc_name)
        # Fallback to platform-specific names
        elif sys.platform == 'darwin':
            return ctypes.CDLL('libc.dylib')
        else:
            return ctypes.CDLL('libc.so.6')


_libc = _get_libc()


# Error codes
class ErrorCode(IntEnum):
    """TidesDB error codes."""
    TDB_SUCCESS = 0
    TDB_ERROR = -1
    TDB_ERR_MEMORY = -2
    TDB_ERR_INVALID_ARGS = -3
    TDB_ERR_IO = -4
    TDB_ERR_NOT_FOUND = -5
    TDB_ERR_EXISTS = -6
    TDB_ERR_CORRUPT = -7
    TDB_ERR_LOCK = -8
    TDB_ERR_TXN_COMMITTED = -9
    TDB_ERR_TXN_ABORTED = -10
    TDB_ERR_READONLY = -11
    TDB_ERR_FULL = -12
    TDB_ERR_INVALID_NAME = -13
    TDB_ERR_COMPARATOR_NOT_FOUND = -14
    TDB_ERR_MAX_COMPARATORS = -15
    TDB_ERR_INVALID_CF = -16
    TDB_ERR_THREAD = -17
    TDB_ERR_CHECKSUM = -18
    TDB_ERR_KEY_DELETED = -19
    TDB_ERR_KEY_EXPIRED = -20


class CompressionAlgo(IntEnum):
    """Compression algorithm types (matches compress_type enum)."""
    SNAPPY = 0  # COMPRESS_SNAPPY
    LZ4 = 1     # COMPRESS_LZ4
    ZSTD = 2    # COMPRESS_ZSTD


class SyncMode(IntEnum):
    """Sync modes for durability."""
    NONE = 0        # Fastest, least durable (OS handles flushing)
    BACKGROUND = 1  # Balanced (fsync every N milliseconds)
    FULL = 2        # Most durable (fsync on every write)


class TidesDBException(Exception):
    """Base exception for TidesDB errors."""
    
    def __init__(self, message: str, code: int = ErrorCode.TDB_ERROR):
        super().__init__(message)
        self.code = code
    
    @classmethod
    def from_code(cls, code: int, context: str = "") -> 'TidesDBException':
        """Create exception from error code."""
        error_messages = {
            ErrorCode.TDB_ERR_MEMORY: "memory allocation failed",
            ErrorCode.TDB_ERR_INVALID_ARGS: "invalid arguments",
            ErrorCode.TDB_ERR_IO: "I/O error",
            ErrorCode.TDB_ERR_NOT_FOUND: "not found",
            ErrorCode.TDB_ERR_EXISTS: "already exists",
            ErrorCode.TDB_ERR_CORRUPT: "data corruption",
            ErrorCode.TDB_ERR_LOCK: "lock acquisition failed",
            ErrorCode.TDB_ERR_TXN_COMMITTED: "transaction already committed",
            ErrorCode.TDB_ERR_TXN_ABORTED: "transaction aborted",
            ErrorCode.TDB_ERR_READONLY: "read-only transaction",
            ErrorCode.TDB_ERR_FULL: "database full",
            ErrorCode.TDB_ERR_INVALID_NAME: "invalid name",
            ErrorCode.TDB_ERR_COMPARATOR_NOT_FOUND: "comparator not found",
            ErrorCode.TDB_ERR_MAX_COMPARATORS: "max comparators reached",
            ErrorCode.TDB_ERR_INVALID_CF: "invalid column family",
            ErrorCode.TDB_ERR_THREAD: "thread operation failed",
            ErrorCode.TDB_ERR_CHECKSUM: "checksum verification failed",
            ErrorCode.TDB_ERR_KEY_DELETED: "key is deleted (tombstone)",
            ErrorCode.TDB_ERR_KEY_EXPIRED: "key has expired (TTL)",
        }
        
        msg = error_messages.get(code, "unknown error")
        if context:
            msg = f"{context}: {msg} (code: {code})"
        else:
            msg = f"{msg} (code: {code})"
        
        return cls(msg, code)


# C structures
class CConfig(Structure):
    """C structure for tidesdb_config_t."""
    _fields_ = [
        ("db_path", ctypes.c_char * 1024),  # TDB_MAX_PATH_LENGTH = 1024
        ("enable_debug_logging", c_int),
        ("max_open_file_handles", c_int),
    ]


class CColumnFamilyConfig(Structure):
    """C structure for tidesdb_column_family_config_t."""
    _fields_ = [
        ("memtable_flush_size", c_size_t),
        ("max_sstables_before_compaction", c_int),
        ("compaction_threads", c_int),
        ("max_level", c_int),
        ("probability", c_float),
        ("compressed", c_int),
        ("compress_algo", c_int),
        ("bloom_filter_fp_rate", c_double),
        ("enable_background_compaction", c_int),
        ("background_compaction_interval", c_int),
        ("use_sbha", c_int),
        ("sync_mode", c_int),
        ("sync_interval", c_int),
        ("comparator_name", c_char_p),
    ]


class CColumnFamilyStat(Structure):
    """C structure for tidesdb_column_family_stat_t."""
    _fields_ = [
        ("name", ctypes.c_char * 256),  # TDB_MAX_CF_NAME_LENGTH
        ("comparator_name", ctypes.c_char * 64),  # TDB_MAX_COMPARATOR_NAME
        ("num_sstables", c_int),
        ("total_sstable_size", c_size_t),
        ("memtable_size", c_size_t),
        ("memtable_entries", c_int),
        ("config", CColumnFamilyConfig),
    ]


# Function signatures
_lib.tidesdb_open.argtypes = [POINTER(CConfig), POINTER(c_void_p)]
_lib.tidesdb_open.restype = c_int

_lib.tidesdb_close.argtypes = [c_void_p]
_lib.tidesdb_close.restype = c_int

_lib.tidesdb_default_column_family_config.argtypes = []
_lib.tidesdb_default_column_family_config.restype = CColumnFamilyConfig

_lib.tidesdb_create_column_family.argtypes = [c_void_p, c_char_p, POINTER(CColumnFamilyConfig)]
_lib.tidesdb_create_column_family.restype = c_int

_lib.tidesdb_drop_column_family.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_drop_column_family.restype = c_int

_lib.tidesdb_get_column_family.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_get_column_family.restype = c_void_p

_lib.tidesdb_list_column_families.argtypes = [c_void_p, POINTER(POINTER(c_char_p)), POINTER(c_int)]
_lib.tidesdb_list_column_families.restype = c_int

_lib.tidesdb_get_column_family_stats.argtypes = [c_void_p, c_char_p, POINTER(POINTER(CColumnFamilyStat))]
_lib.tidesdb_get_column_family_stats.restype = c_int

_lib.tidesdb_compact.argtypes = [c_void_p]
_lib.tidesdb_compact.restype = c_int

_lib.tidesdb_txn_begin.argtypes = [c_void_p, POINTER(c_void_p)]
_lib.tidesdb_txn_begin.restype = c_int

_lib.tidesdb_txn_begin_read.argtypes = [c_void_p, POINTER(c_void_p)]
_lib.tidesdb_txn_begin_read.restype = c_int

_lib.tidesdb_txn_put.argtypes = [c_void_p, c_char_p, POINTER(ctypes.c_uint8), c_size_t, 
                                  POINTER(ctypes.c_uint8), c_size_t, c_int64]
_lib.tidesdb_txn_put.restype = c_int

_lib.tidesdb_txn_get.argtypes = [c_void_p, c_char_p, POINTER(ctypes.c_uint8), c_size_t,
                                  POINTER(POINTER(ctypes.c_uint8)), POINTER(c_size_t)]
_lib.tidesdb_txn_get.restype = c_int

_lib.tidesdb_txn_delete.argtypes = [c_void_p, c_char_p, POINTER(ctypes.c_uint8), c_size_t]
_lib.tidesdb_txn_delete.restype = c_int

_lib.tidesdb_txn_commit.argtypes = [c_void_p]
_lib.tidesdb_txn_commit.restype = c_int

_lib.tidesdb_txn_rollback.argtypes = [c_void_p]
_lib.tidesdb_txn_rollback.restype = c_int

_lib.tidesdb_txn_free.argtypes = [c_void_p]
_lib.tidesdb_txn_free.restype = None

_lib.tidesdb_iter_new.argtypes = [c_void_p, c_char_p, POINTER(c_void_p)]
_lib.tidesdb_iter_new.restype = c_int

_lib.tidesdb_iter_seek_to_first.argtypes = [c_void_p]
_lib.tidesdb_iter_seek_to_first.restype = c_int

_lib.tidesdb_iter_seek_to_last.argtypes = [c_void_p]
_lib.tidesdb_iter_seek_to_last.restype = c_int

_lib.tidesdb_iter_valid.argtypes = [c_void_p]
_lib.tidesdb_iter_valid.restype = c_int

_lib.tidesdb_iter_next.argtypes = [c_void_p]
_lib.tidesdb_iter_next.restype = c_int

_lib.tidesdb_iter_prev.argtypes = [c_void_p]
_lib.tidesdb_iter_prev.restype = c_int

_lib.tidesdb_iter_key.argtypes = [c_void_p, POINTER(POINTER(ctypes.c_uint8)), POINTER(c_size_t)]
_lib.tidesdb_iter_key.restype = c_int

_lib.tidesdb_iter_value.argtypes = [c_void_p, POINTER(POINTER(ctypes.c_uint8)), POINTER(c_size_t)]
_lib.tidesdb_iter_value.restype = c_int

_lib.tidesdb_iter_free.argtypes = [c_void_p]
_lib.tidesdb_iter_free.restype = None


class ColumnFamilyConfig:
    """Configuration for a column family."""
    
    def __init__(
        self,
        memtable_flush_size: int = 67108864,  # 64MB
        max_sstables_before_compaction: int = 128,
        compaction_threads: int = 4,
        max_level: int = 12,
        probability: float = 0.25,
        compressed: bool = True,
        compress_algo: CompressionAlgo = CompressionAlgo.SNAPPY,
        bloom_filter_fp_rate: float = 0.01,
        enable_background_compaction: bool = True,
        background_compaction_interval: int = 1000000,  # 1 second in microseconds
        use_sbha: bool = True,
        sync_mode: SyncMode = SyncMode.BACKGROUND,
        sync_interval: int = 1000,
        comparator_name: Optional[str] = None
    ):
        """
        Initialize column family configuration.
        
        Args:
            memtable_flush_size: Size threshold for memtable flush (default 64MB)
            max_sstables_before_compaction: Trigger compaction at this many SSTables (default 128)
            compaction_threads: Number of threads for parallel compaction (default 4, 0=single-threaded)
            max_level: Skip list max level (default 12)
            probability: Skip list probability (default 0.25)
            compressed: Enable compression (default True)
            compress_algo: Compression algorithm (default SNAPPY)
            bloom_filter_fp_rate: Bloom filter false positive rate (default 0.01)
            enable_background_compaction: Enable automatic background compaction (default True)
            background_compaction_interval: Interval in microseconds between compaction checks (default 1000000 = 1 second)
            use_sbha: Use sorted binary hash array for fast lookups (default True)
            sync_mode: Durability sync mode (default BACKGROUND)
            sync_interval: Sync interval in milliseconds for BACKGROUND mode (default 1000)
            comparator_name: Name of custom comparator or None for default "memcmp"
        """
        self.memtable_flush_size = memtable_flush_size
        self.max_sstables_before_compaction = max_sstables_before_compaction
        self.compaction_threads = compaction_threads
        self.max_level = max_level
        self.probability = probability
        self.compressed = compressed
        self.compress_algo = compress_algo
        self.bloom_filter_fp_rate = bloom_filter_fp_rate
        self.enable_background_compaction = enable_background_compaction
        self.background_compaction_interval = background_compaction_interval
        self.use_sbha = use_sbha
        self.sync_mode = sync_mode
        self.sync_interval = sync_interval
        self.comparator_name = comparator_name
    
    @classmethod
    def default(cls) -> 'ColumnFamilyConfig':
        """Get default column family configuration."""
        c_config = _lib.tidesdb_default_column_family_config()
        return cls(
            memtable_flush_size=c_config.memtable_flush_size,
            max_sstables_before_compaction=c_config.max_sstables_before_compaction,
            compaction_threads=c_config.compaction_threads,
            max_level=c_config.max_level,
            probability=c_config.probability,
            compressed=bool(c_config.compressed),
            compress_algo=CompressionAlgo(c_config.compress_algo),
            bloom_filter_fp_rate=c_config.bloom_filter_fp_rate,
            enable_background_compaction=bool(c_config.enable_background_compaction),
            background_compaction_interval=c_config.background_compaction_interval,
            use_sbha=bool(c_config.use_sbha),
            sync_mode=SyncMode(c_config.sync_mode),
            sync_interval=c_config.sync_interval,
            comparator_name=None
        )
    
    def _to_c_struct(self) -> CColumnFamilyConfig:
        """Convert to C structure."""
        c_config = CColumnFamilyConfig()
        c_config.memtable_flush_size = self.memtable_flush_size
        c_config.max_sstables_before_compaction = self.max_sstables_before_compaction
        c_config.compaction_threads = self.compaction_threads
        c_config.max_level = self.max_level
        c_config.probability = self.probability
        c_config.compressed = 1 if self.compressed else 0
        c_config.compress_algo = int(self.compress_algo)
        c_config.bloom_filter_fp_rate = self.bloom_filter_fp_rate
        c_config.enable_background_compaction = 1 if self.enable_background_compaction else 0
        c_config.background_compaction_interval = self.background_compaction_interval
        c_config.use_sbha = 1 if self.use_sbha else 0
        c_config.sync_mode = int(self.sync_mode)
        c_config.sync_interval = self.sync_interval
        c_config.comparator_name = self.comparator_name.encode() if self.comparator_name else None
        return c_config


class ColumnFamilyStat:
    """Statistics for a column family."""
    
    def __init__(
        self,
        name: str,
        comparator_name: str,
        num_sstables: int,
        total_sstable_size: int,
        memtable_size: int,
        memtable_entries: int,
        config: ColumnFamilyConfig
    ):
        self.name = name
        self.comparator_name = comparator_name
        self.num_sstables = num_sstables
        self.total_sstable_size = total_sstable_size
        self.memtable_size = memtable_size
        self.memtable_entries = memtable_entries
        self.config = config
    
    def __repr__(self) -> str:
        return (
            f"ColumnFamilyStat(name={self.name!r}, comparator={self.comparator_name!r}, "
            f"sstables={self.num_sstables}, memtable_entries={self.memtable_entries})"
        )


class Iterator:
    """Iterator for traversing key-value pairs in a column family."""
    
    def __init__(self, iter_ptr: c_void_p):
        """Initialize iterator with C pointer."""
        self._iter = iter_ptr
        self._closed = False
    
    def seek_to_first(self) -> None:
        """Position iterator at the first key."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        _lib.tidesdb_iter_seek_to_first(self._iter)
    
    def seek_to_last(self) -> None:
        """Position iterator at the last key."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        _lib.tidesdb_iter_seek_to_last(self._iter)
    
    def valid(self) -> bool:
        """Check if iterator is positioned at a valid entry."""
        if self._closed:
            return False
        return bool(_lib.tidesdb_iter_valid(self._iter))
    
    def next(self) -> None:
        """Move iterator to the next entry."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        _lib.tidesdb_iter_next(self._iter)
    
    def prev(self) -> None:
        """Move iterator to the previous entry."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        _lib.tidesdb_iter_prev(self._iter)
    
    def key(self) -> bytes:
        """Get the current key."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        
        key_ptr = POINTER(ctypes.c_uint8)()
        key_size = c_size_t()
        
        result = _lib.tidesdb_iter_key(self._iter, ctypes.byref(key_ptr), ctypes.byref(key_size))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to get key")
        
        # key_ptr points to internal iterator memory, do NOT free it
        return ctypes.string_at(key_ptr, key_size.value)
    
    def value(self) -> bytes:
        """Get the current value."""
        if self._closed:
            raise TidesDBException("Iterator is closed")
        
        value_ptr = POINTER(ctypes.c_uint8)()
        value_size = c_size_t()
        
        result = _lib.tidesdb_iter_value(self._iter, ctypes.byref(value_ptr), ctypes.byref(value_size))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to get value")
        
        # value_ptr points to internal iterator memory, do NOT free it
        return ctypes.string_at(value_ptr, value_size.value)
    
    def items(self) -> List[Tuple[bytes, bytes]]:
        """Get all remaining items as a list of (key, value) tuples."""
        results = []
        while self.valid():
            results.append((self.key(), self.value()))
            self.next()
        return results
    
    def close(self) -> None:
        """Free iterator resources."""
        if not self._closed:
            _lib.tidesdb_iter_free(self._iter)
            self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def __iter__(self):
        return self
    
    def __next__(self) -> Tuple[bytes, bytes]:
        if not self.valid():
            raise StopIteration
        key = self.key()
        value = self.value()
        self.next()
        return key, value
    
    def __del__(self):
        self.close()


class Transaction:
    """Transaction for atomic operations."""
    
    def __init__(self, txn_ptr: c_void_p):
        """Initialize transaction with C pointer."""
        self._txn = txn_ptr
        self._closed = False
        self._committed = False
    
    def put(self, column_family: str, key: bytes, value: bytes, ttl: int = -1) -> None:
        """
        Put a key-value pair in the transaction.
        
        Args:
            column_family: Name of the column family
            key: Key as bytes
            value: Value as bytes
            ttl: Time-to-live as Unix timestamp, or -1 for no expiration
        """
        if self._closed:
            raise TidesDBException("Transaction is closed")
        if self._committed:
            raise TidesDBException("Transaction already committed")
        
        cf_name = column_family.encode()
        key_buf = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        value_buf = (ctypes.c_uint8 * len(value)).from_buffer_copy(value)
        
        result = _lib.tidesdb_txn_put(
            self._txn, cf_name,
            key_buf, len(key),
            value_buf, len(value),
            ttl
        )
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to put key-value pair")
    
    def get(self, column_family: str, key: bytes) -> bytes:
        """
        Get a value from the transaction.
        
        Args:
            column_family: Name of the column family
            key: Key as bytes
        
        Returns:
            Value as bytes
        """
        if self._closed:
            raise TidesDBException("Transaction is closed")
        
        cf_name = column_family.encode()
        key_buf = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        value_ptr = POINTER(ctypes.c_uint8)()
        value_size = c_size_t()
        
        result = _lib.tidesdb_txn_get(
            self._txn, cf_name,
            key_buf, len(key),
            ctypes.byref(value_ptr), ctypes.byref(value_size)
        )
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to get value")
        
        value = ctypes.string_at(value_ptr, value_size.value)
        
        # Free the malloc'd value (C API allocates with malloc)
        _libc.free(ctypes.cast(value_ptr, ctypes.c_void_p))
        return value
    
    def delete(self, column_family: str, key: bytes) -> None:
        """
        Delete a key-value pair in the transaction.
        
        Args:
            column_family: Name of the column family
            key: Key as bytes
        """
        if self._closed:
            raise TidesDBException("Transaction is closed")
        if self._committed:
            raise TidesDBException("Transaction already committed")
        
        cf_name = column_family.encode()
        key_buf = (ctypes.c_uint8 * len(key)).from_buffer_copy(key)
        
        result = _lib.tidesdb_txn_delete(self._txn, cf_name, key_buf, len(key))
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to delete key")
    
    def commit(self) -> None:
        """Commit the transaction."""
        if self._closed:
            raise TidesDBException("Transaction is closed")
        if self._committed:
            raise TidesDBException("Transaction already committed")
        
        result = _lib.tidesdb_txn_commit(self._txn)
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to commit transaction")
        
        self._committed = True
    
    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._closed:
            raise TidesDBException("Transaction is closed")
        if self._committed:
            raise TidesDBException("Transaction already committed")
        
        result = _lib.tidesdb_txn_rollback(self._txn)
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to rollback transaction")
    
    def new_iterator(self, column_family: str) -> Iterator:
        """
        Create a new iterator for the column family.
        
        Args:
            column_family: Name of the column family
        
        Returns:
            Iterator instance
        """
        if self._closed:
            raise TidesDBException("Transaction is closed")
        
        cf_name = column_family.encode()
        iter_ptr = c_void_p()
        
        result = _lib.tidesdb_iter_new(self._txn, cf_name, ctypes.byref(iter_ptr))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to create iterator")
        
        return Iterator(iter_ptr)
    
    def close(self) -> None:
        """Free transaction resources."""
        if not self._closed:
            _lib.tidesdb_txn_free(self._txn)
            self._closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not self._committed:
            try:
                self.rollback()
            except:
                pass
        self.close()
        return False
    
    def __del__(self):
        self.close()


class ColumnFamily:
    """Column family handle."""
    
    def __init__(self, cf_ptr: c_void_p, name: str):
        """Initialize column family with C pointer."""
        self._cf = cf_ptr
        self.name = name
    
    def compact(self) -> None:
        """
        Manually trigger compaction for this column family.
        Requires minimum 2 SSTables to merge.
        Uses parallel compaction if compaction_threads > 0.
        """
        result = _lib.tidesdb_compact(self._cf)
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to compact column family")


class TidesDB:
    """TidesDB database instance."""
    
    def __init__(self, path: str, enable_debug_logging: bool = False, max_open_file_handles: int = 0):
        """Open a TidesDB database.
        
        Args:
            path: Path to the database directory
            enable_debug_logging: Enable debug logging to stderr
            max_open_file_handles: Maximum number of open file handles to cache (0 = unlimited)
        """
        import os
        
        # Initialize state first
        self._db = None
        self._closed = False
        
        # Create directory if it doesn't exist
        os.makedirs(path, exist_ok=True)
        
        # Convert to absolute path
        abs_path = os.path.abspath(path)
        
        # Encode path and ensure it fits in TDB_MAX_PATH_LENGTH (1024 bytes)
        path_bytes = abs_path.encode('utf-8')
        if len(path_bytes) >= 1024:
            raise ValueError(f"Database path too long (max 1023 bytes): {abs_path}")
        
        # Create config with fixed-size char array
        self._config = CConfig(
            enable_debug_logging=1 if enable_debug_logging else 0,
            max_open_file_handles=max_open_file_handles
        )
        # Copy path into the fixed-size array
        self._config.db_path = path_bytes
        
        db_ptr = c_void_p()
        result = _lib.tidesdb_open(ctypes.byref(self._config), ctypes.byref(db_ptr))
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to open database")
        
        self._db = db_ptr
    
    def close(self) -> None:
        """Close the database."""
        if not self._closed and self._db:
            result = _lib.tidesdb_close(self._db)
            if result != ErrorCode.TDB_SUCCESS:
                raise TidesDBException.from_code(result, "failed to close database")
            self._closed = True
    
    def create_column_family(self, name: str, config: Optional[ColumnFamilyConfig] = None) -> None:
        """
        Create a new column family.
        
        Args:
            name: Name of the column family
            config: Configuration for the column family, or None for defaults
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        if config is None:
            config = ColumnFamilyConfig.default()
        
        cf_name = name.encode()
        c_config = config._to_c_struct()
        
        result = _lib.tidesdb_create_column_family(self._db, cf_name, ctypes.byref(c_config))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to create column family")
    
    def drop_column_family(self, name: str) -> None:
        """
        Drop a column family and all its data.
        
        Args:
            name: Name of the column family
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        cf_name = name.encode()
        result = _lib.tidesdb_drop_column_family(self._db, cf_name)
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to drop column family")
    
    def get_column_family(self, name: str) -> ColumnFamily:
        """
        Get a column family handle.
        
        Args:
            name: Name of the column family
        
        Returns:
            ColumnFamily instance
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        cf_name = name.encode()
        cf_ptr = _lib.tidesdb_get_column_family(self._db, cf_name)
        
        if not cf_ptr:
            raise TidesDBException(f"Column family not found: {name}", ErrorCode.TDB_ERR_NOT_FOUND)
        
        return ColumnFamily(cf_ptr, name)
    
    def list_column_families(self) -> List[str]:
        """
        List all column families in the database.
        
        Returns:
            List of column family names
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        names_array_ptr = ctypes.POINTER(c_char_p)()
        count = c_int()
        
        result = _lib.tidesdb_list_column_families(self._db, ctypes.byref(names_array_ptr), ctypes.byref(count))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to list column families")
        
        if count.value == 0:
            return []
        
        names = []
        
        # Copy all strings first before freeing anything
        for i in range(count.value):
            # names_array_ptr[i] automatically dereferences to get the char* value
            name_bytes = names_array_ptr[i]
            if name_bytes:
                names.append(name_bytes.decode('utf-8'))
        
        # Now free each string pointer
        # We need to reinterpret the array as void pointers to free them
        void_ptr_array = ctypes.cast(names_array_ptr, ctypes.POINTER(ctypes.c_void_p))
        for i in range(count.value):
            ptr = void_ptr_array[i]
            if ptr:
                _libc.free(ptr)
        
        # Free the array itself
        _libc.free(ctypes.cast(names_array_ptr, ctypes.c_void_p))
        
        return names
    
    def get_column_family_stats(self, name: str) -> ColumnFamilyStat:
        """
        Get statistics for a column family.
        
        Args:
            name: Name of the column family
        
        Returns:
            ColumnFamilyStat instance
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        cf_name = name.encode()
        stats_ptr = POINTER(CColumnFamilyStat)()
        
        result = _lib.tidesdb_get_column_family_stats(self._db, cf_name, ctypes.byref(stats_ptr))
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to get column family stats")
        
        c_stats = stats_ptr.contents
        
        config = ColumnFamilyConfig(
            memtable_flush_size=c_stats.config.memtable_flush_size,
            max_sstables_before_compaction=c_stats.config.max_sstables_before_compaction,
            compaction_threads=c_stats.config.compaction_threads,
            max_level=c_stats.config.max_level,
            probability=c_stats.config.probability,
            compressed=bool(c_stats.config.compressed),
            compress_algo=CompressionAlgo(c_stats.config.compress_algo),
            bloom_filter_fp_rate=c_stats.config.bloom_filter_fp_rate,
            enable_background_compaction=bool(c_stats.config.enable_background_compaction),
            background_compaction_interval=c_stats.config.background_compaction_interval,
            use_sbha=bool(c_stats.config.use_sbha),
            sync_mode=SyncMode(c_stats.config.sync_mode),
            sync_interval=c_stats.config.sync_interval,
        )
        
        stats = ColumnFamilyStat(
            name=c_stats.name.decode('utf-8').rstrip('\x00'),
            comparator_name=c_stats.comparator_name.decode('utf-8').rstrip('\x00'),
            num_sstables=c_stats.num_sstables,
            total_sstable_size=c_stats.total_sstable_size,
            memtable_size=c_stats.memtable_size,
            memtable_entries=c_stats.memtable_entries,
            config=config
        )
        
        # Free the malloc'd stats structure (C API requires caller to free)
        _libc.free(ctypes.cast(stats_ptr, ctypes.c_void_p))
        return stats
    
    def begin_txn(self) -> Transaction:
        """
        Begin a new write transaction.
        
        Returns:
            Transaction instance
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        txn_ptr = c_void_p()
        result = _lib.tidesdb_txn_begin(self._db, ctypes.byref(txn_ptr))
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to begin transaction")
        
        return Transaction(txn_ptr)
    
    def begin_read_txn(self) -> Transaction:
        """
        Begin a new read-only transaction.
        
        Returns:
            Transaction instance
        """
        if self._closed:
            raise TidesDBException("Database is closed")
        
        txn_ptr = c_void_p()
        result = _lib.tidesdb_txn_begin_read(self._db, ctypes.byref(txn_ptr))
        
        if result != ErrorCode.TDB_SUCCESS:
            raise TidesDBException.from_code(result, "failed to begin read transaction")
        
        return Transaction(txn_ptr)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
    
    def __del__(self):
        if not self._closed:
            try:
                self.close()
            except:
                pass


__all__ = [
    'TidesDB',
    'Transaction',
    'Iterator',
    'ColumnFamily',
    'ColumnFamilyConfig',
    'ColumnFamilyStat',
    'CompressionAlgo',
    'SyncMode',
    'ErrorCode',
    'TidesDBException',
]