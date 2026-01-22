"""
TidesDB Python Bindings v7+

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

from __future__ import annotations

import ctypes
import os
import sys
from ctypes import (
    POINTER,
    Structure,
    c_char,
    c_char_p,
    c_double,
    c_float,
    c_int,
    c_size_t,
    c_uint8,
    c_uint64,
    c_void_p,
)
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterator as TypingIterator


def _load_library() -> ctypes.CDLL:
    """Load the TidesDB shared library."""
    if sys.platform == "win32":
        lib_names = ["tidesdb.dll", "libtidesdb.dll"]
    elif sys.platform == "darwin":
        lib_names = ["libtidesdb.dylib", "libtidesdb.so"]
    else:
        lib_names = ["libtidesdb.so", "libtidesdb.so.1"]

    search_paths = [
        "",
        "/usr/local/lib/",
        "/usr/lib/",
        "/opt/homebrew/lib/",
        "/mingw64/lib/",
    ]

    for path in search_paths:
        for lib_name in lib_names:
            try:
                return ctypes.CDLL(path + lib_name)
            except OSError:
                continue

    raise RuntimeError(
        "Could not load TidesDB library. "
        "Please ensure libtidesdb is installed and in your library path. "
        "On Linux: /usr/local/lib or set LD_LIBRARY_PATH. "
        "On macOS: /usr/local/lib or /opt/homebrew/lib or set DYLD_LIBRARY_PATH. "
        "On Windows: ensure tidesdb.dll is in PATH or current directory."
    )


_lib = _load_library()


TDB_MAX_COMPARATOR_NAME = 64
TDB_MAX_COMPARATOR_CTX = 256

TDB_SUCCESS = 0
TDB_ERR_MEMORY = -1
TDB_ERR_INVALID_ARGS = -2
TDB_ERR_NOT_FOUND = -3
TDB_ERR_IO = -4
TDB_ERR_CORRUPTION = -5
TDB_ERR_EXISTS = -6
TDB_ERR_CONFLICT = -7
TDB_ERR_TOO_LARGE = -8
TDB_ERR_MEMORY_LIMIT = -9
TDB_ERR_INVALID_DB = -10
TDB_ERR_UNKNOWN = -11
TDB_ERR_LOCKED = -12


class CompressionAlgorithm(IntEnum):
    """Compression algorithm types."""

    NO_COMPRESSION = 0
    SNAPPY_COMPRESSION = 1
    LZ4_COMPRESSION = 2
    ZSTD_COMPRESSION = 3
    LZ4_FAST_COMPRESSION = 4


class SyncMode(IntEnum):
    """Sync modes for durability."""

    SYNC_NONE = 0
    SYNC_FULL = 1
    SYNC_INTERVAL = 2


class LogLevel(IntEnum):
    """Logging levels."""

    LOG_DEBUG = 0
    LOG_INFO = 1
    LOG_WARN = 2
    LOG_ERROR = 3
    LOG_FATAL = 4
    LOG_NONE = 99


class IsolationLevel(IntEnum):
    """Transaction isolation levels."""

    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1
    REPEATABLE_READ = 2
    SNAPSHOT = 3
    SERIALIZABLE = 4


class TidesDBError(Exception):
    """Base exception for TidesDB errors."""

    def __init__(self, message: str, code: int = TDB_ERR_UNKNOWN):
        super().__init__(message)
        self.code = code

    @classmethod
    def from_code(cls, code: int, context: str = "") -> TidesDBError:
        """Create exception from error code."""
        error_messages = {
            TDB_ERR_MEMORY: "memory allocation failed",
            TDB_ERR_INVALID_ARGS: "invalid arguments",
            TDB_ERR_NOT_FOUND: "not found",
            TDB_ERR_IO: "I/O error",
            TDB_ERR_CORRUPTION: "data corruption",
            TDB_ERR_EXISTS: "already exists",
            TDB_ERR_CONFLICT: "transaction conflict",
            TDB_ERR_TOO_LARGE: "key or value too large",
            TDB_ERR_MEMORY_LIMIT: "memory limit exceeded",
            TDB_ERR_INVALID_DB: "invalid database handle",
            TDB_ERR_UNKNOWN: "unknown error",
            TDB_ERR_LOCKED: "database is locked",
        }

        msg = error_messages.get(code, "unknown error")
        if context:
            msg = f"{context}: {msg} (code: {code})"
        else:
            msg = f"{msg} (code: {code})"

        return cls(msg, code)


class _CColumnFamilyConfig(Structure):
    """C structure for tidesdb_column_family_config_t."""

    _fields_ = [
        ("write_buffer_size", c_size_t),
        ("level_size_ratio", c_size_t),
        ("min_levels", c_int),
        ("dividing_level_offset", c_int),
        ("klog_value_threshold", c_size_t),
        ("compression_algo", c_int),
        ("enable_bloom_filter", c_int),
        ("bloom_fpr", c_double),
        ("enable_block_indexes", c_int),
        ("index_sample_ratio", c_int),
        ("block_index_prefix_len", c_int),
        ("sync_mode", c_int),
        ("sync_interval_us", c_uint64),
        ("comparator_name", c_char * TDB_MAX_COMPARATOR_NAME),
        ("comparator_ctx_str", c_char * TDB_MAX_COMPARATOR_CTX),
        ("comparator_fn_cached", c_void_p),
        ("comparator_ctx_cached", c_void_p),
        ("skip_list_max_level", c_int),
        ("skip_list_probability", c_float),
        ("default_isolation_level", c_int),
        ("min_disk_space", c_uint64),
        ("l1_file_count_trigger", c_int),
        ("l0_queue_stall_threshold", c_int),
    ]


class _CConfig(Structure):
    """C structure for tidesdb_config_t."""

    _fields_ = [
        ("db_path", c_char_p),
        ("num_flush_threads", c_int),
        ("num_compaction_threads", c_int),
        ("log_level", c_int),
        ("block_cache_size", c_size_t),
        ("max_open_sstables", c_size_t),
    ]


class _CStats(Structure):
    """C structure for tidesdb_stats_t."""

    _fields_ = [
        ("num_levels", c_int),
        ("memtable_size", c_size_t),
        ("level_sizes", POINTER(c_size_t)),
        ("level_num_sstables", POINTER(c_int)),
        ("config", POINTER(_CColumnFamilyConfig)),
    ]


class _CCacheStats(Structure):
    """C structure for tidesdb_cache_stats_t."""

    _fields_ = [
        ("enabled", c_int),
        ("total_entries", c_size_t),
        ("total_bytes", c_size_t),
        ("hits", c_uint64),
        ("misses", c_uint64),
        ("hit_rate", c_double),
        ("num_partitions", c_size_t),
    ]


_lib.tidesdb_default_column_family_config.argtypes = []
_lib.tidesdb_default_column_family_config.restype = _CColumnFamilyConfig

_lib.tidesdb_default_config.argtypes = []
_lib.tidesdb_default_config.restype = _CConfig

_lib.tidesdb_open.argtypes = [POINTER(_CConfig), POINTER(c_void_p)]
_lib.tidesdb_open.restype = c_int

_lib.tidesdb_close.argtypes = [c_void_p]
_lib.tidesdb_close.restype = c_int

_lib.tidesdb_create_column_family.argtypes = [c_void_p, c_char_p, POINTER(_CColumnFamilyConfig)]
_lib.tidesdb_create_column_family.restype = c_int

_lib.tidesdb_drop_column_family.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_drop_column_family.restype = c_int

_lib.tidesdb_get_column_family.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_get_column_family.restype = c_void_p

_lib.tidesdb_list_column_families.argtypes = [c_void_p, POINTER(POINTER(c_char_p)), POINTER(c_int)]
_lib.tidesdb_list_column_families.restype = c_int

_lib.tidesdb_txn_begin.argtypes = [c_void_p, POINTER(c_void_p)]
_lib.tidesdb_txn_begin.restype = c_int

_lib.tidesdb_txn_begin_with_isolation.argtypes = [c_void_p, c_int, POINTER(c_void_p)]
_lib.tidesdb_txn_begin_with_isolation.restype = c_int

_lib.tidesdb_txn_put.argtypes = [
    c_void_p,
    c_void_p,
    POINTER(c_uint8),
    c_size_t,
    POINTER(c_uint8),
    c_size_t,
    c_int,
]
_lib.tidesdb_txn_put.restype = c_int

_lib.tidesdb_txn_get.argtypes = [
    c_void_p,
    c_void_p,
    POINTER(c_uint8),
    c_size_t,
    POINTER(POINTER(c_uint8)),
    POINTER(c_size_t),
]
_lib.tidesdb_txn_get.restype = c_int

_lib.tidesdb_txn_delete.argtypes = [c_void_p, c_void_p, POINTER(c_uint8), c_size_t]
_lib.tidesdb_txn_delete.restype = c_int

_lib.tidesdb_txn_commit.argtypes = [c_void_p]
_lib.tidesdb_txn_commit.restype = c_int

_lib.tidesdb_txn_rollback.argtypes = [c_void_p]
_lib.tidesdb_txn_rollback.restype = c_int

_lib.tidesdb_txn_free.argtypes = [c_void_p]
_lib.tidesdb_txn_free.restype = None

_lib.tidesdb_txn_savepoint.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_txn_savepoint.restype = c_int

_lib.tidesdb_txn_rollback_to_savepoint.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_txn_rollback_to_savepoint.restype = c_int

_lib.tidesdb_txn_release_savepoint.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_txn_release_savepoint.restype = c_int

_lib.tidesdb_iter_new.argtypes = [c_void_p, c_void_p, POINTER(c_void_p)]
_lib.tidesdb_iter_new.restype = c_int

_lib.tidesdb_iter_seek_to_first.argtypes = [c_void_p]
_lib.tidesdb_iter_seek_to_first.restype = c_int

_lib.tidesdb_iter_seek_to_last.argtypes = [c_void_p]
_lib.tidesdb_iter_seek_to_last.restype = c_int

_lib.tidesdb_iter_seek.argtypes = [c_void_p, POINTER(c_uint8), c_size_t]
_lib.tidesdb_iter_seek.restype = c_int

_lib.tidesdb_iter_seek_for_prev.argtypes = [c_void_p, POINTER(c_uint8), c_size_t]
_lib.tidesdb_iter_seek_for_prev.restype = c_int

_lib.tidesdb_iter_valid.argtypes = [c_void_p]
_lib.tidesdb_iter_valid.restype = c_int

_lib.tidesdb_iter_next.argtypes = [c_void_p]
_lib.tidesdb_iter_next.restype = c_int

_lib.tidesdb_iter_prev.argtypes = [c_void_p]
_lib.tidesdb_iter_prev.restype = c_int

_lib.tidesdb_iter_key.argtypes = [c_void_p, POINTER(POINTER(c_uint8)), POINTER(c_size_t)]
_lib.tidesdb_iter_key.restype = c_int

_lib.tidesdb_iter_value.argtypes = [c_void_p, POINTER(POINTER(c_uint8)), POINTER(c_size_t)]
_lib.tidesdb_iter_value.restype = c_int

_lib.tidesdb_iter_free.argtypes = [c_void_p]
_lib.tidesdb_iter_free.restype = None

_lib.tidesdb_compact.argtypes = [c_void_p]
_lib.tidesdb_compact.restype = c_int

_lib.tidesdb_flush_memtable.argtypes = [c_void_p]
_lib.tidesdb_flush_memtable.restype = c_int

_lib.tidesdb_get_stats.argtypes = [c_void_p, POINTER(POINTER(_CStats))]
_lib.tidesdb_get_stats.restype = c_int

_lib.tidesdb_free_stats.argtypes = [POINTER(_CStats)]
_lib.tidesdb_free_stats.restype = None

_lib.tidesdb_get_cache_stats.argtypes = [c_void_p, POINTER(_CCacheStats)]
_lib.tidesdb_get_cache_stats.restype = c_int


@dataclass
class Config:
    """Configuration for opening a TidesDB instance."""

    db_path: str
    num_flush_threads: int = 2
    num_compaction_threads: int = 2
    log_level: LogLevel = LogLevel.LOG_INFO
    block_cache_size: int = 64 * 1024 * 1024
    max_open_sstables: int = 256


@dataclass
class ColumnFamilyConfig:
    """Configuration for a column family."""

    write_buffer_size: int = 64 * 1024 * 1024
    level_size_ratio: int = 10
    min_levels: int = 5
    dividing_level_offset: int = 2
    klog_value_threshold: int = 512
    compression_algorithm: CompressionAlgorithm = CompressionAlgorithm.LZ4_COMPRESSION
    enable_bloom_filter: bool = True
    bloom_fpr: float = 0.01
    enable_block_indexes: bool = True
    index_sample_ratio: int = 1
    block_index_prefix_len: int = 16
    sync_mode: SyncMode = SyncMode.SYNC_INTERVAL
    sync_interval_us: int = 128000
    comparator_name: str = "memcmp"
    skip_list_max_level: int = 12
    skip_list_probability: float = 0.25
    default_isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED
    min_disk_space: int = 100 * 1024 * 1024
    l1_file_count_trigger: int = 4
    l0_queue_stall_threshold: int = 20

    def _to_c_struct(self) -> _CColumnFamilyConfig:
        """Convert to C structure."""
        c_config = _CColumnFamilyConfig()
        c_config.write_buffer_size = self.write_buffer_size
        c_config.level_size_ratio = self.level_size_ratio
        c_config.min_levels = self.min_levels
        c_config.dividing_level_offset = self.dividing_level_offset
        c_config.klog_value_threshold = self.klog_value_threshold
        c_config.compression_algo = int(self.compression_algorithm)
        c_config.enable_bloom_filter = 1 if self.enable_bloom_filter else 0
        c_config.bloom_fpr = self.bloom_fpr
        c_config.enable_block_indexes = 1 if self.enable_block_indexes else 0
        c_config.index_sample_ratio = self.index_sample_ratio
        c_config.block_index_prefix_len = self.block_index_prefix_len
        c_config.sync_mode = int(self.sync_mode)
        c_config.sync_interval_us = self.sync_interval_us
        c_config.skip_list_max_level = self.skip_list_max_level
        c_config.skip_list_probability = self.skip_list_probability
        c_config.default_isolation_level = int(self.default_isolation_level)
        c_config.min_disk_space = self.min_disk_space
        c_config.l1_file_count_trigger = self.l1_file_count_trigger
        c_config.l0_queue_stall_threshold = self.l0_queue_stall_threshold

        name_bytes = self.comparator_name.encode("utf-8")[:TDB_MAX_COMPARATOR_NAME - 1]
        name_bytes = name_bytes + b"\x00" * (TDB_MAX_COMPARATOR_NAME - len(name_bytes))
        c_config.comparator_name = name_bytes

        return c_config


@dataclass
class Stats:
    """Statistics about a column family."""

    num_levels: int
    memtable_size: int
    level_sizes: list[int]
    level_num_sstables: list[int]
    config: ColumnFamilyConfig | None = None


@dataclass
class CacheStats:
    """Statistics about the block cache."""

    enabled: bool
    total_entries: int
    total_bytes: int
    hits: int
    misses: int
    hit_rate: float
    num_partitions: int


def default_config() -> Config:
    """Get default database configuration."""
    return Config(db_path="")


def default_column_family_config() -> ColumnFamilyConfig:
    """Get default column family configuration from C library."""
    c_config = _lib.tidesdb_default_column_family_config()
    return ColumnFamilyConfig(
        write_buffer_size=c_config.write_buffer_size,
        level_size_ratio=c_config.level_size_ratio,
        min_levels=c_config.min_levels,
        dividing_level_offset=c_config.dividing_level_offset,
        klog_value_threshold=c_config.klog_value_threshold,
        compression_algorithm=CompressionAlgorithm(c_config.compression_algo),
        enable_bloom_filter=bool(c_config.enable_bloom_filter),
        bloom_fpr=c_config.bloom_fpr,
        enable_block_indexes=bool(c_config.enable_block_indexes),
        index_sample_ratio=c_config.index_sample_ratio,
        block_index_prefix_len=c_config.block_index_prefix_len,
        sync_mode=SyncMode(c_config.sync_mode),
        sync_interval_us=c_config.sync_interval_us,
        comparator_name=c_config.comparator_name.decode("utf-8").rstrip("\x00"),
        skip_list_max_level=c_config.skip_list_max_level,
        skip_list_probability=c_config.skip_list_probability,
        default_isolation_level=IsolationLevel(c_config.default_isolation_level),
        min_disk_space=c_config.min_disk_space,
        l1_file_count_trigger=c_config.l1_file_count_trigger,
        l0_queue_stall_threshold=c_config.l0_queue_stall_threshold,
    )


class Iterator:
    """Iterator for traversing key-value pairs in a column family."""

    def __init__(self, iter_ptr: c_void_p) -> None:
        self._iter = iter_ptr
        self._closed = False

    def seek_to_first(self) -> None:
        """Position iterator at the first key."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        result = _lib.tidesdb_iter_seek_to_first(self._iter)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to seek to first")

    def seek_to_last(self) -> None:
        """Position iterator at the last key."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        result = _lib.tidesdb_iter_seek_to_last(self._iter)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to seek to last")

    def seek(self, key: bytes) -> None:
        """Position iterator at the first key >= target key."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        key_buf = (c_uint8 * len(key)).from_buffer_copy(key) if key else None
        result = _lib.tidesdb_iter_seek(self._iter, key_buf, len(key))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to seek")

    def seek_for_prev(self, key: bytes) -> None:
        """Position iterator at the last key <= target key."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        key_buf = (c_uint8 * len(key)).from_buffer_copy(key) if key else None
        result = _lib.tidesdb_iter_seek_for_prev(self._iter, key_buf, len(key))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to seek for prev")

    def valid(self) -> bool:
        """Check if iterator is positioned at a valid entry."""
        if self._closed:
            return False
        return bool(_lib.tidesdb_iter_valid(self._iter))

    def next(self) -> None:
        """Move iterator to the next entry."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        # next() returns NOT_FOUND when reaching the end, which is not an error
        _lib.tidesdb_iter_next(self._iter)

    def prev(self) -> None:
        """Move iterator to the previous entry."""
        if self._closed:
            raise TidesDBError("Iterator is closed")
        # prev() returns NOT_FOUND when reaching the beginning, which is not an error
        _lib.tidesdb_iter_prev(self._iter)

    def key(self) -> bytes:
        """Get the current key."""
        if self._closed:
            raise TidesDBError("Iterator is closed")

        key_ptr = POINTER(c_uint8)()
        key_size = c_size_t()

        result = _lib.tidesdb_iter_key(self._iter, ctypes.byref(key_ptr), ctypes.byref(key_size))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get key")

        return ctypes.string_at(key_ptr, key_size.value)

    def value(self) -> bytes:
        """Get the current value."""
        if self._closed:
            raise TidesDBError("Iterator is closed")

        value_ptr = POINTER(c_uint8)()
        value_size = c_size_t()

        result = _lib.tidesdb_iter_value(
            self._iter, ctypes.byref(value_ptr), ctypes.byref(value_size)
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get value")

        return ctypes.string_at(value_ptr, value_size.value)

    def close(self) -> None:
        """Free iterator resources."""
        if not self._closed and self._iter:
            _lib.tidesdb_iter_free(self._iter)
            self._closed = True

    def __enter__(self) -> Iterator:
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object) -> bool:
        self.close()
        return False

    def __iter__(self) -> TypingIterator[tuple[bytes, bytes]]:
        return self

    def __next__(self) -> tuple[bytes, bytes]:
        if not self.valid():
            raise StopIteration
        key = self.key()
        value = self.value()
        self.next()
        return key, value

    def __del__(self) -> None:
        if _lib is None:
            return
        self.close()


class ColumnFamily:
    """Column family handle."""

    def __init__(self, cf_ptr: c_void_p, name: str) -> None:
        self._cf = cf_ptr
        self.name = name

    def compact(self) -> None:
        """Manually trigger compaction for this column family."""
        result = _lib.tidesdb_compact(self._cf)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to compact column family")

    def flush_memtable(self) -> None:
        """Manually trigger memtable flush for this column family."""
        result = _lib.tidesdb_flush_memtable(self._cf)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to flush memtable")

    def get_stats(self) -> Stats:
        """Get statistics for this column family."""
        stats_ptr = POINTER(_CStats)()
        result = _lib.tidesdb_get_stats(self._cf, ctypes.byref(stats_ptr))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get stats")

        c_stats = stats_ptr.contents

        level_sizes = []
        level_num_sstables = []

        if c_stats.num_levels > 0:
            if c_stats.level_sizes:
                for i in range(c_stats.num_levels):
                    level_sizes.append(c_stats.level_sizes[i])
            if c_stats.level_num_sstables:
                for i in range(c_stats.num_levels):
                    level_num_sstables.append(c_stats.level_num_sstables[i])

        config = None
        if c_stats.config:
            c_cfg = c_stats.config.contents
            config = ColumnFamilyConfig(
                write_buffer_size=c_cfg.write_buffer_size,
                level_size_ratio=c_cfg.level_size_ratio,
                min_levels=c_cfg.min_levels,
                dividing_level_offset=c_cfg.dividing_level_offset,
                klog_value_threshold=c_cfg.klog_value_threshold,
                compression_algorithm=CompressionAlgorithm(c_cfg.compression_algo),
                enable_bloom_filter=bool(c_cfg.enable_bloom_filter),
                bloom_fpr=c_cfg.bloom_fpr,
                enable_block_indexes=bool(c_cfg.enable_block_indexes),
                index_sample_ratio=c_cfg.index_sample_ratio,
                block_index_prefix_len=c_cfg.block_index_prefix_len,
                sync_mode=SyncMode(c_cfg.sync_mode),
                sync_interval_us=c_cfg.sync_interval_us,
                comparator_name=c_cfg.comparator_name.decode("utf-8").rstrip("\x00"),
                skip_list_max_level=c_cfg.skip_list_max_level,
                skip_list_probability=c_cfg.skip_list_probability,
                default_isolation_level=IsolationLevel(c_cfg.default_isolation_level),
                min_disk_space=c_cfg.min_disk_space,
                l1_file_count_trigger=c_cfg.l1_file_count_trigger,
                l0_queue_stall_threshold=c_cfg.l0_queue_stall_threshold,
            )

        stats = Stats(
            num_levels=c_stats.num_levels,
            memtable_size=c_stats.memtable_size,
            level_sizes=level_sizes,
            level_num_sstables=level_num_sstables,
            config=config,
        )

        _lib.tidesdb_free_stats(stats_ptr)
        return stats


class Transaction:
    """Transaction for atomic operations."""

    def __init__(self, txn_ptr: c_void_p) -> None:
        self._txn = txn_ptr
        self._closed = False
        self._committed = False
        self._freed = False

    def put(self, cf: ColumnFamily, key: bytes, value: bytes, ttl: int = -1) -> None:
        """
        Put a key-value pair in the transaction.

        Args:
            cf: Column family handle
            key: Key as bytes
            value: Value as bytes
            ttl: Time-to-live as Unix timestamp (seconds since epoch), or -1 for no expiration
        """
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        key_buf = (c_uint8 * len(key)).from_buffer_copy(key) if key else None
        value_buf = (c_uint8 * len(value)).from_buffer_copy(value) if value else None

        result = _lib.tidesdb_txn_put(
            self._txn, cf._cf, key_buf, len(key), value_buf, len(value), ttl
        )

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to put key-value pair")

    def get(self, cf: ColumnFamily, key: bytes) -> bytes:
        """
        Get a value from the transaction.

        Args:
            cf: Column family handle
            key: Key as bytes

        Returns:
            Value as bytes

        Raises:
            TidesDBError: If key not found or other error
        """
        if self._closed:
            raise TidesDBError("Transaction is closed")

        key_buf = (c_uint8 * len(key)).from_buffer_copy(key) if key else None
        value_ptr = POINTER(c_uint8)()
        value_size = c_size_t()

        result = _lib.tidesdb_txn_get(
            self._txn, cf._cf, key_buf, len(key), ctypes.byref(value_ptr), ctypes.byref(value_size)
        )

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get value")

        value = ctypes.string_at(value_ptr, value_size.value)

        libc = ctypes.CDLL(None)
        libc.free(ctypes.cast(value_ptr, c_void_p))

        return value

    def delete(self, cf: ColumnFamily, key: bytes) -> None:
        """
        Delete a key-value pair in the transaction.

        Args:
            cf: Column family handle
            key: Key as bytes
        """
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        key_buf = (c_uint8 * len(key)).from_buffer_copy(key) if key else None

        result = _lib.tidesdb_txn_delete(self._txn, cf._cf, key_buf, len(key))

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to delete key")

    def commit(self) -> None:
        """Commit the transaction."""
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        result = _lib.tidesdb_txn_commit(self._txn)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to commit transaction")

        self._committed = True

    def rollback(self) -> None:
        """Rollback the transaction."""
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        result = _lib.tidesdb_txn_rollback(self._txn)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to rollback transaction")

    def savepoint(self, name: str) -> None:
        """Create a savepoint within the transaction."""
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        result = _lib.tidesdb_txn_savepoint(self._txn, name.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to create savepoint")

    def rollback_to_savepoint(self, name: str) -> None:
        """Rollback the transaction to a savepoint."""
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        result = _lib.tidesdb_txn_rollback_to_savepoint(self._txn, name.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to rollback to savepoint")

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint without rolling back."""
        if self._closed:
            raise TidesDBError("Transaction is closed")
        if self._committed:
            raise TidesDBError("Transaction already committed")

        result = _lib.tidesdb_txn_release_savepoint(self._txn, name.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to release savepoint")

    def new_iterator(self, cf: ColumnFamily) -> Iterator:
        """
        Create a new iterator for the column family within this transaction.

        Args:
            cf: Column family handle

        Returns:
            Iterator instance
        """
        if self._closed:
            raise TidesDBError("Transaction is closed")

        iter_ptr = c_void_p()
        result = _lib.tidesdb_iter_new(self._txn, cf._cf, ctypes.byref(iter_ptr))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to create iterator")

        return Iterator(iter_ptr)

    def close(self) -> None:
        """Free transaction resources."""
        if not self._closed and self._txn:
            _lib.tidesdb_txn_free(self._txn)
            self._closed = True

    def __enter__(self) -> Transaction:
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object) -> bool:
        if exc_type is not None and not self._committed:
            try:
                self.rollback()
            except TidesDBError:
                pass
        self.close()
        return False

    def __del__(self) -> None:
        if _lib is None:
            return
        self.close()


class TidesDB:
    """TidesDB database instance."""

    def __init__(self, config: Config) -> None:
        """
        Open a TidesDB database.

        Args:
            config: Database configuration
        """
        self._db: c_void_p | None = None
        self._closed = False

        os.makedirs(config.db_path, exist_ok=True)
        abs_path = os.path.abspath(config.db_path)

        self._path_bytes = abs_path.encode("utf-8")

        c_config = _CConfig(
            db_path=self._path_bytes,
            num_flush_threads=config.num_flush_threads,
            num_compaction_threads=config.num_compaction_threads,
            log_level=int(config.log_level),
            block_cache_size=config.block_cache_size,
            max_open_sstables=config.max_open_sstables,
        )

        db_ptr = c_void_p()
        result = _lib.tidesdb_open(ctypes.byref(c_config), ctypes.byref(db_ptr))

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to open database")

        self._db = db_ptr

    @classmethod
    def open(
        cls,
        path: str,
        num_flush_threads: int = 2,
        num_compaction_threads: int = 2,
        log_level: LogLevel = LogLevel.LOG_INFO,
        block_cache_size: int = 64 * 1024 * 1024,
        max_open_sstables: int = 256,
    ) -> TidesDB:
        """
        Convenience method to open a database with individual parameters.

        Args:
            path: Path to the database directory
            num_flush_threads: Number of flush threads
            num_compaction_threads: Number of compaction threads
            log_level: Logging level
            block_cache_size: Size of block cache in bytes
            max_open_sstables: Maximum number of open SSTables

        Returns:
            TidesDB instance
        """
        config = Config(
            db_path=path,
            num_flush_threads=num_flush_threads,
            num_compaction_threads=num_compaction_threads,
            log_level=log_level,
            block_cache_size=block_cache_size,
            max_open_sstables=max_open_sstables,
        )
        return cls(config)

    def close(self) -> None:
        """Close the database."""
        if _lib is None:
            return
        if not self._closed and self._db:
            db_ptr = self._db
            self._db = None
            self._closed = True
            result = _lib.tidesdb_close(db_ptr)
            if result != TDB_SUCCESS:
                raise TidesDBError.from_code(result, "failed to close database")

    def create_column_family(
        self, name: str, config: ColumnFamilyConfig | None = None
    ) -> None:
        """
        Create a new column family.

        Args:
            name: Name of the column family
            config: Configuration for the column family, or None for defaults
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        if config is None:
            config = default_column_family_config()

        c_config = config._to_c_struct()

        result = _lib.tidesdb_create_column_family(
            self._db, name.encode("utf-8"), ctypes.byref(c_config)
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to create column family")

    def drop_column_family(self, name: str) -> None:
        """
        Drop a column family and all its data.

        Args:
            name: Name of the column family
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        result = _lib.tidesdb_drop_column_family(self._db, name.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to drop column family")

    def get_column_family(self, name: str) -> ColumnFamily:
        """
        Get a column family handle.

        Args:
            name: Name of the column family

        Returns:
            ColumnFamily instance
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        cf_ptr = _lib.tidesdb_get_column_family(self._db, name.encode("utf-8"))
        if not cf_ptr:
            raise TidesDBError(f"Column family not found: {name}", TDB_ERR_NOT_FOUND)

        return ColumnFamily(cf_ptr, name)

    def list_column_families(self) -> list[str]:
        """
        List all column families in the database.

        Returns:
            List of column family names
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        names_ptr = POINTER(c_char_p)()
        count = c_int()

        result = _lib.tidesdb_list_column_families(
            self._db, ctypes.byref(names_ptr), ctypes.byref(count)
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to list column families")

        if count.value == 0:
            return []

        names = []

        try:
            if sys.platform == "win32":
                libc = ctypes.CDLL("msvcrt")
            elif sys.platform == "darwin":
                libc = ctypes.CDLL("libc.dylib")
            else:
                libc = ctypes.CDLL("libc.so.6")
            libc.free.argtypes = [c_void_p]
            libc.free.restype = None
        except OSError:
            libc = None

        raw_array = ctypes.cast(names_ptr, POINTER(c_void_p))

        for i in range(count.value):
            str_ptr = raw_array[i]
            if str_ptr:
                char_ptr = ctypes.cast(str_ptr, c_char_p)
                names.append(char_ptr.value.decode("utf-8"))
                if libc:
                    libc.free(str_ptr)

        if libc:
            libc.free(ctypes.cast(names_ptr, c_void_p))

        return names

    def begin_txn(self) -> Transaction:
        """
        Begin a new transaction with default isolation level.

        Returns:
            Transaction instance
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        txn_ptr = c_void_p()
        result = _lib.tidesdb_txn_begin(self._db, ctypes.byref(txn_ptr))

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to begin transaction")

        return Transaction(txn_ptr)

    def begin_txn_with_isolation(self, isolation: IsolationLevel) -> Transaction:
        """
        Begin a new transaction with the specified isolation level.

        Args:
            isolation: Transaction isolation level

        Returns:
            Transaction instance
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        txn_ptr = c_void_p()
        result = _lib.tidesdb_txn_begin_with_isolation(
            self._db, int(isolation), ctypes.byref(txn_ptr)
        )

        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to begin transaction with isolation")

        return Transaction(txn_ptr)

    def get_cache_stats(self) -> CacheStats:
        """
        Get statistics about the block cache.

        Returns:
            CacheStats instance
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        c_stats = _CCacheStats()
        result = _lib.tidesdb_get_cache_stats(self._db, ctypes.byref(c_stats))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get cache stats")

        return CacheStats(
            enabled=bool(c_stats.enabled),
            total_entries=c_stats.total_entries,
            total_bytes=c_stats.total_bytes,
            hits=c_stats.hits,
            misses=c_stats.misses,
            hit_rate=c_stats.hit_rate,
            num_partitions=c_stats.num_partitions,
        )

    def __enter__(self) -> TidesDB:
        return self

    def __exit__(self, exc_type: type | None, exc_val: Exception | None, exc_tb: object) -> bool:
        self.close()
        return False

    def __del__(self) -> None:
        if _lib is None:
            return
        if not self._closed:
            try:
                self.close()
            except (TidesDBError, OSError):
                pass
