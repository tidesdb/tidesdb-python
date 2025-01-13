"""
TidesDB Python Bindings Core Module
"""
import ctypes
import ctypes.util
from ctypes import (
    c_char_p, c_int, c_float, c_bool, c_size_t,
    c_uint8, c_time_t, POINTER, byref, create_string_buffer
)

class TidesDBCompressionAlgo:
    NO_COMPRESSION = 0
    COMPRESS_SNAPPY = 1
    COMPRESS_LZ4 = 2
    COMPRESS_ZSTD = 3

class TidesDBMemtableDS:
    SKIP_LIST = 0
    HASH_TABLE = 1

library_name = 'tidesdb'
library_path = ctypes.util.find_library(library_name)

if library_path:
    lib = ctypes.CDLL(library_path)
else:
    raise FileNotFoundError(f"Library '{library_name}' not found")

class TidesDB:
    """TidesDB main database class."""
    
    def __init__(self, tdb):
        self.tdb = tdb

    @staticmethod
    def open(directory: str) -> 'TidesDB':
        c_dir = create_string_buffer(directory.encode('utf-8'))
        tdb = POINTER(ctypes.c_void_p)()
        result = lib.tidesdb_open(c_dir, byref(tdb))
        if result != 0:
            raise Exception("Failed to open TidesDB")
        return TidesDB(tdb)

    def close(self):
        if hasattr(self, 'tdb'):
            result = lib.tidesdb_close(self.tdb)
            if result != 0:
                raise Exception("Failed to close TidesDB")

    def create_column_family(self, name, flush_threshold, max_level, probability, 
                        compressed, compress_algo, bloom_filter, memtable_ds):
        c_name = create_string_buffer(name.encode('utf-8'))
        result = lib.tidesdb_create_column_family(
            self.tdb, c_name, c_int(flush_threshold), c_int(max_level),
            c_float(probability), c_bool(compressed), c_int(compress_algo),
            c_bool(bloom_filter), c_int(memtable_ds)
        )
        if result != 0:
            raise Exception("Failed to create column family")

    def put(self, column_family_name, key, value, ttl):
        c_name = create_string_buffer(column_family_name.encode('utf-8'))
        c_key = (c_uint8 * len(key)).from_buffer_copy(key)
        c_value = (c_uint8 * len(value)).from_buffer_copy(value)
        result = lib.tidesdb_put(
            self.tdb, c_name, c_key, c_size_t(len(key)),
            c_value, c_size_t(len(value)), c_time_t(ttl)
        )
        if result != 0:
            raise Exception("Failed to put key-value pair")

    def get(self, column_family_name, key):
        c_name = create_string_buffer(column_family_name.encode('utf-8'))
        c_key = (c_uint8 * len(key)).from_buffer_copy(key)
        c_value = POINTER(c_uint8)()
        c_value_size = c_size_t()
        result = lib.tidesdb_get(
            self.tdb, c_name, c_key, c_size_t(len(key)),
            byref(c_value), byref(c_value_size)
        )
        if result != 0:
            raise Exception("Failed to get value")
        return bytes((c_uint8 * c_value_size.value).from_address(ctypes.addressof(c_value.contents)))

    def delete(self, column_family_name, key):
        c_name = create_string_buffer(column_family_name.encode('utf-8'))
        c_key = (c_uint8 * len(key)).from_buffer_copy(key)
        result = lib.tidesdb_delete(self.tdb, c_name, c_key, c_size_t(len(key)))
        if result != 0:
            raise Exception("Failed to delete key-value pair")

class Cursor:
    """Cursor class for iterating over column family key-value pairs."""
    
    def __init__(self, cursor):
        self.cursor = cursor

    @staticmethod
    def init(db: TidesDB, column_family: str) -> 'Cursor':
        c_name = create_string_buffer(column_family.encode('utf-8'))
        cursor = POINTER(ctypes.c_void_p)()
        result = lib.tidesdb_cursor_init(db.tdb, c_name, byref(cursor))
        if result != 0:
            raise Exception("Failed to initialize cursor")
        return Cursor(cursor)

    def next(self):
        result = lib.tidesdb_cursor_next(self.cursor)
        if result != 0:
            raise Exception("Failed to move cursor to next")

    def prev(self):
        result = lib.tidesdb_cursor_prev(self.cursor)
        if result != 0:
            raise Exception("Failed to move cursor to previous")

    def get(self):
        c_key = POINTER(c_uint8)()
        c_key_size = c_size_t()
        c_value = POINTER(c_uint8)()
        c_value_size = c_size_t()
        result = lib.tidesdb_cursor_get(self.cursor, byref(c_key), byref(c_key_size),
                                      byref(c_value), byref(c_value_size))
        if result != 0:
            raise Exception("Failed to get key-value pair from cursor")
        key = bytes((c_uint8 * c_key_size.value).from_address(ctypes.addressof(c_key.contents)))
        value = bytes((c_uint8 * c_value_size.value).from_address(ctypes.addressof(c_value.contents)))
        return key, value

    def free(self):
        result = lib.tidesdb_cursor_free(self.cursor)
        if result != 0:
            raise Exception("Failed to free cursor")

class Transaction:
    """Transaction class for atomic operations on column families."""
    
    def __init__(self, txn):
        self.txn = txn

    @staticmethod
    def begin(db: TidesDB, column_family: str) -> 'Transaction':
        c_name = create_string_buffer(column_family.encode('utf-8'))
        txn = POINTER(ctypes.c_void_p)()
        result = lib.tidesdb_txn_begin(db.tdb, byref(txn), c_name)
        if result != 0:
            raise Exception("Failed to begin transaction")
        return Transaction(txn)

    def put(self, key, value, ttl):
        c_key = (c_uint8 * len(key)).from_buffer_copy(key)
        c_value = (c_uint8 * len(value)).from_buffer_copy(value)
        result = lib.tidesdb_txn_put(self.txn, c_key, c_size_t(len(key)),
                                    c_value, c_size_t(len(value)), c_time_t(ttl))
        if result != 0:
            raise Exception("Failed to put key-value pair in transaction")

    def delete(self, key):
        c_key = (c_uint8 * len(key)).from_buffer_copy(key)
        result = lib.tidesdb_txn_delete(self.txn, c_key, c_size_t(len(key)))
        if result != 0:
            raise Exception("Failed to delete key-value pair from transaction")

    def commit(self):
        result = lib.tidesdb_txn_commit(self.txn)
        if result != 0:
            raise Exception("Failed to commit transaction")

    def rollback(self):
        result = lib.tidesdb_txn_rollback(self.txn)
        if result != 0:
            raise Exception("Failed to rollback transaction")

    def free(self):
        result = lib.tidesdb_txn_free(self.txn)
        if result != 0:
            raise Exception("Failed to free transaction")