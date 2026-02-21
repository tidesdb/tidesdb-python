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
    CFUNCTYPE,
    POINTER,
    Structure,
    c_char,
    c_char_p,
    c_double,
    c_float,
    c_int,
    c_int64,
    c_size_t,
    c_uint8,
    c_uint32,
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
TDB_MAX_CF_NAME_LEN = 128

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
        ("name", c_char * TDB_MAX_CF_NAME_LEN),
        ("write_buffer_size", c_size_t),
        ("level_size_ratio", c_size_t),
        ("min_levels", c_int),
        ("dividing_level_offset", c_int),
        ("klog_value_threshold", c_size_t),
        ("compression_algorithm", c_int),
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
        ("use_btree", c_int),
        ("commit_hook_fn", c_void_p),
        ("commit_hook_ctx", c_void_p),
    ]


class _CCommitOp(Structure):
    """C structure for tidesdb_commit_op_t."""

    _fields_ = [
        ("key", POINTER(c_uint8)),
        ("key_size", c_size_t),
        ("value", POINTER(c_uint8)),
        ("value_size", c_size_t),
        ("ttl", c_int64),
        ("is_delete", c_int),
    ]


# Commit hook callback: int (*)(const tidesdb_commit_op_t*, int, uint64_t, void*)
COMMIT_HOOK_FUNC = CFUNCTYPE(c_int, POINTER(_CCommitOp), c_int, c_uint64, c_void_p)


class _CConfig(Structure):
    """C structure for tidesdb_config_t."""

    _fields_ = [
        ("db_path", c_char_p),
        ("num_flush_threads", c_int),
        ("num_compaction_threads", c_int),
        ("log_level", c_int),
        ("block_cache_size", c_size_t),
        ("max_open_sstables", c_size_t),
        ("log_to_file", c_int),
        ("log_truncation_at", c_size_t),
    ]


class _CStats(Structure):
    """C structure for tidesdb_stats_t."""

    _fields_ = [
        ("num_levels", c_int),
        ("memtable_size", c_size_t),
        ("level_sizes", POINTER(c_size_t)),
        ("level_num_sstables", POINTER(c_int)),
        ("config", POINTER(_CColumnFamilyConfig)),
        ("total_keys", c_uint64),
        ("total_data_size", c_uint64),
        ("avg_key_size", c_double),
        ("avg_value_size", c_double),
        ("level_key_counts", POINTER(c_uint64)),
        ("read_amp", c_double),
        ("hit_rate", c_double),
        ("use_btree", c_int),
        ("btree_total_nodes", c_uint64),
        ("btree_max_height", c_uint32),
        ("btree_avg_height", c_double),
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
    c_int64,
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

_lib.tidesdb_backup.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_backup.restype = c_int

_lib.tidesdb_checkpoint.argtypes = [c_void_p, c_char_p]
_lib.tidesdb_checkpoint.restype = c_int

_lib.tidesdb_rename_column_family.argtypes = [c_void_p, c_char_p, c_char_p]
_lib.tidesdb_rename_column_family.restype = c_int

_lib.tidesdb_clone_column_family.argtypes = [c_void_p, c_char_p, c_char_p]
_lib.tidesdb_clone_column_family.restype = c_int

_lib.tidesdb_txn_reset.argtypes = [c_void_p, c_int]
_lib.tidesdb_txn_reset.restype = c_int

_lib.tidesdb_cf_update_runtime_config.argtypes = [c_void_p, POINTER(_CColumnFamilyConfig), c_int]
_lib.tidesdb_cf_update_runtime_config.restype = c_int

_lib.tidesdb_cf_config_save_to_ini.argtypes = [c_char_p, c_char_p, POINTER(_CColumnFamilyConfig)]
_lib.tidesdb_cf_config_save_to_ini.restype = c_int

_lib.tidesdb_is_flushing.argtypes = [c_void_p]
_lib.tidesdb_is_flushing.restype = c_int

_lib.tidesdb_is_compacting.argtypes = [c_void_p]
_lib.tidesdb_is_compacting.restype = c_int

_lib.tidesdb_range_cost.argtypes = [
    c_void_p,
    POINTER(c_uint8),
    c_size_t,
    POINTER(c_uint8),
    c_size_t,
    POINTER(c_double),
]
_lib.tidesdb_range_cost.restype = c_int

_lib.tidesdb_cf_config_load_from_ini.argtypes = [c_char_p, c_char_p, POINTER(_CColumnFamilyConfig)]
_lib.tidesdb_cf_config_load_from_ini.restype = c_int

_lib.tidesdb_cf_set_commit_hook.argtypes = [c_void_p, COMMIT_HOOK_FUNC, c_void_p]
_lib.tidesdb_cf_set_commit_hook.restype = c_int

# Comparator function type: int (*)(const uint8_t*, size_t, const uint8_t*, size_t, void*)
COMPARATOR_FUNC = ctypes.CFUNCTYPE(c_int, POINTER(c_uint8), c_size_t, POINTER(c_uint8), c_size_t, c_void_p)
DESTROY_FUNC = ctypes.CFUNCTYPE(None, c_void_p)

_lib.tidesdb_register_comparator.argtypes = [c_void_p, c_char_p, COMPARATOR_FUNC, c_void_p, DESTROY_FUNC]
_lib.tidesdb_register_comparator.restype = c_int


@dataclass
class Config:
    """Configuration for opening a TidesDB instance."""

    db_path: str
    num_flush_threads: int = 2
    num_compaction_threads: int = 2
    log_level: LogLevel = LogLevel.LOG_INFO
    block_cache_size: int = 64 * 1024 * 1024
    max_open_sstables: int = 256
    log_to_file: bool = False
    log_truncation_at: int = 24 * 1024 * 1024


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
    use_btree: bool = False

    def _to_c_struct(self, name: str = "") -> _CColumnFamilyConfig:
        """Convert to C structure."""
        c_config = _CColumnFamilyConfig()

        # Set the name field
        cf_name_bytes = name.encode("utf-8")[:TDB_MAX_CF_NAME_LEN - 1]
        cf_name_bytes = cf_name_bytes + b"\x00" * (TDB_MAX_CF_NAME_LEN - len(cf_name_bytes))
        c_config.name = cf_name_bytes

        c_config.write_buffer_size = self.write_buffer_size
        c_config.level_size_ratio = self.level_size_ratio
        c_config.min_levels = self.min_levels
        c_config.dividing_level_offset = self.dividing_level_offset
        c_config.klog_value_threshold = self.klog_value_threshold
        c_config.compression_algorithm = int(self.compression_algorithm)
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
        c_config.use_btree = 1 if self.use_btree else 0

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
    total_keys: int
    total_data_size: int
    avg_key_size: float
    avg_value_size: float
    level_key_counts: list[int]
    read_amp: float
    hit_rate: float
    use_btree: bool = False
    btree_total_nodes: int = 0
    btree_max_height: int = 0
    btree_avg_height: float = 0.0
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


@dataclass
class CommitOp:
    """A single operation from a committed transaction batch."""

    key: bytes
    value: bytes | None
    ttl: int
    is_delete: bool


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
        compression_algorithm=CompressionAlgorithm(c_config.compression_algorithm),
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
        use_btree=bool(c_config.use_btree),
    )


def save_config_to_ini(file_path: str, cf_name: str, config: ColumnFamilyConfig) -> None:
    """
    Save column family configuration to a custom INI file.

    Args:
        file_path: Path to the INI file to create/overwrite
        cf_name: Name of the column family (used as section name)
        config: Configuration to save
    """
    c_config = config._to_c_struct(cf_name)
    result = _lib.tidesdb_cf_config_save_to_ini(
        file_path.encode("utf-8"), cf_name.encode("utf-8"), ctypes.byref(c_config)
    )
    if result != TDB_SUCCESS:
        raise TidesDBError.from_code(result, "failed to save config to INI file")


def load_config_from_ini(file_path: str, cf_name: str) -> ColumnFamilyConfig:
    """
    Load column family configuration from an INI file.

    Args:
        file_path: Path to the INI file to read
        cf_name: Name of the section to read (column family name)

    Returns:
        ColumnFamilyConfig populated from the INI file
    """
    c_config = _CColumnFamilyConfig()
    result = _lib.tidesdb_cf_config_load_from_ini(
        file_path.encode("utf-8"), cf_name.encode("utf-8"), ctypes.byref(c_config)
    )
    if result != TDB_SUCCESS:
        raise TidesDBError.from_code(result, "failed to load config from INI file")

    return ColumnFamilyConfig(
        write_buffer_size=c_config.write_buffer_size,
        level_size_ratio=c_config.level_size_ratio,
        min_levels=c_config.min_levels,
        dividing_level_offset=c_config.dividing_level_offset,
        klog_value_threshold=c_config.klog_value_threshold,
        compression_algorithm=CompressionAlgorithm(c_config.compression_algorithm),
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
        use_btree=bool(c_config.use_btree),
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

    def is_flushing(self) -> bool:
        """Check if a flush operation is in progress for this column family."""
        return bool(_lib.tidesdb_is_flushing(self._cf))

    def is_compacting(self) -> bool:
        """Check if a compaction operation is in progress for this column family."""
        return bool(_lib.tidesdb_is_compacting(self._cf))

    def update_runtime_config(self, config: ColumnFamilyConfig, persist_to_disk: bool = True) -> None:
        """
        Update runtime-safe configuration settings for this column family.

        Updatable settings (safe to change at runtime):
        - write_buffer_size: Memtable flush threshold
        - skip_list_max_level: Skip list level for new memtables
        - skip_list_probability: Skip list probability for new memtables
        - bloom_fpr: False positive rate for new SSTables
        - index_sample_ratio: Index sampling ratio for new SSTables
        - sync_mode: Durability mode
        - sync_interval_us: Sync interval in microseconds

        Args:
            config: New configuration settings
            persist_to_disk: If True, save changes to config.ini
        """
        c_config = config._to_c_struct(self.name)
        result = _lib.tidesdb_cf_update_runtime_config(
            self._cf, ctypes.byref(c_config), 1 if persist_to_disk else 0
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to update runtime config")

    def set_commit_hook(self, callback: callable) -> None:
        """
        Set a commit hook (change data capture) callback for this column family.

        The callback fires synchronously after every transaction commit on this
        column family. It receives the full batch of committed operations
        atomically, enabling real-time change data capture.

        The callback signature is:
            callback(ops: list[CommitOp], commit_seq: int) -> int

        Return 0 from the callback on success. A non-zero return is logged as a
        warning but does not roll back the commit.

        Args:
            callback: Python callable with the signature above
        """
        def c_hook(ops_ptr, num_ops, commit_seq, ctx_ptr):
            ops = []
            for i in range(num_ops):
                c_op = ops_ptr[i]
                key = ctypes.string_at(c_op.key, c_op.key_size) if c_op.key and c_op.key_size > 0 else b""
                value = ctypes.string_at(c_op.value, c_op.value_size) if c_op.value and c_op.value_size > 0 else None
                ops.append(CommitOp(
                    key=key,
                    value=value,
                    ttl=c_op.ttl,
                    is_delete=bool(c_op.is_delete),
                ))
            try:
                return callback(ops, commit_seq)
            except Exception:
                return -1

        c_func = COMMIT_HOOK_FUNC(c_hook)

        # Store reference to prevent garbage collection
        if not hasattr(self, "_commit_hook_ref"):
            self._commit_hook_ref = None
        self._commit_hook_ref = c_func

        result = _lib.tidesdb_cf_set_commit_hook(self._cf, c_func, None)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to set commit hook")

    def clear_commit_hook(self) -> None:
        """Disable the commit hook for this column family."""
        result = _lib.tidesdb_cf_set_commit_hook(self._cf, COMMIT_HOOK_FUNC(0), None)
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to clear commit hook")

        if hasattr(self, "_commit_hook_ref"):
            self._commit_hook_ref = None

    def range_cost(self, key_a: bytes, key_b: bytes) -> float:
        """
        Estimate the computational cost of iterating between two keys.

        The returned value is an opaque double meaningful only for comparison
        with other values from the same function. It uses only in-memory metadata
        and performs no disk I/O. Key order does not matter.

        Args:
            key_a: First key (bound of range)
            key_b: Second key (bound of range)

        Returns:
            Estimated traversal cost (higher = more expensive)

        Raises:
            TidesDBError: If arguments are invalid (NULL pointers, zero-length keys)
        """
        key_a_buf = (c_uint8 * len(key_a)).from_buffer_copy(key_a) if key_a else None
        key_b_buf = (c_uint8 * len(key_b)).from_buffer_copy(key_b) if key_b else None
        cost = c_double()

        result = _lib.tidesdb_range_cost(
            self._cf, key_a_buf, len(key_a), key_b_buf, len(key_b), ctypes.byref(cost)
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to estimate range cost")

        return cost.value

    def get_stats(self) -> Stats:
        """Get statistics for this column family."""
        stats_ptr = POINTER(_CStats)()
        result = _lib.tidesdb_get_stats(self._cf, ctypes.byref(stats_ptr))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to get stats")

        c_stats = stats_ptr.contents

        level_sizes = []
        level_num_sstables = []
        level_key_counts = []

        if c_stats.num_levels > 0:
            if c_stats.level_sizes:
                for i in range(c_stats.num_levels):
                    level_sizes.append(c_stats.level_sizes[i])
            if c_stats.level_num_sstables:
                for i in range(c_stats.num_levels):
                    level_num_sstables.append(c_stats.level_num_sstables[i])
            if c_stats.level_key_counts:
                for i in range(c_stats.num_levels):
                    level_key_counts.append(c_stats.level_key_counts[i])

        config = None
        if c_stats.config:
            c_cfg = c_stats.config.contents
            config = ColumnFamilyConfig(
                write_buffer_size=c_cfg.write_buffer_size,
                level_size_ratio=c_cfg.level_size_ratio,
                min_levels=c_cfg.min_levels,
                dividing_level_offset=c_cfg.dividing_level_offset,
                klog_value_threshold=c_cfg.klog_value_threshold,
                compression_algorithm=CompressionAlgorithm(c_cfg.compression_algorithm),
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
                use_btree=bool(c_cfg.use_btree),
            )

        stats = Stats(
            num_levels=c_stats.num_levels,
            memtable_size=c_stats.memtable_size,
            level_sizes=level_sizes,
            level_num_sstables=level_num_sstables,
            total_keys=c_stats.total_keys,
            total_data_size=c_stats.total_data_size,
            avg_key_size=c_stats.avg_key_size,
            avg_value_size=c_stats.avg_value_size,
            level_key_counts=level_key_counts,
            read_amp=c_stats.read_amp,
            hit_rate=c_stats.hit_rate,
            use_btree=bool(c_stats.use_btree),
            btree_total_nodes=c_stats.btree_total_nodes,
            btree_max_height=c_stats.btree_max_height,
            btree_avg_height=c_stats.btree_avg_height,
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

        if sys.platform == "win32":
            libc = ctypes.CDLL("msvcrt")
        else:
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

    def reset(self, isolation: IsolationLevel = IsolationLevel.READ_COMMITTED) -> None:
        """
        Reset a committed or aborted transaction for reuse with a new isolation level.

        This avoids the overhead of freeing and reallocating transaction resources
        in hot loops. The transaction must be committed or rolled back before reset.

        Args:
            isolation: New isolation level for the reset transaction

        Raises:
            TidesDBError: If transaction is still active (not committed/aborted)
        """
        if self._closed:
            raise TidesDBError("Transaction is closed")

        result = _lib.tidesdb_txn_reset(self._txn, int(isolation))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to reset transaction")

        self._committed = False

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
            log_to_file=1 if config.log_to_file else 0,
            log_truncation_at=config.log_truncation_at,
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
        log_to_file: bool = False,
        log_truncation_at: int = 24 * 1024 * 1024,
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
            log_to_file: Write logs to file instead of stderr
            log_truncation_at: Log file truncation size in bytes (0 = no truncation)

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
            log_to_file=log_to_file,
            log_truncation_at=log_truncation_at,
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

        c_config = config._to_c_struct(name)

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

    def backup(self, backup_dir: str) -> None:
        """
        Create an on-disk snapshot of the database without blocking normal reads/writes.

        Args:
            backup_dir: Path to the backup directory (must be non-existent or empty)

        Raises:
            TidesDBError: If backup fails (e.g., directory not empty, I/O error)
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        result = _lib.tidesdb_backup(self._db, backup_dir.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to create backup")

    def checkpoint(self, checkpoint_dir: str) -> None:
        """
        Create a lightweight, near-instant snapshot of the database using hard links.

        Unlike backup(), checkpoint uses hard links instead of copying SSTable data,
        making it near-instant and using no extra disk space until compaction removes
        old SSTables.

        Args:
            checkpoint_dir: Path to the checkpoint directory (must be non-existent or empty)

        Raises:
            TidesDBError: If checkpoint fails (e.g., directory not empty, I/O error)
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        result = _lib.tidesdb_checkpoint(self._db, checkpoint_dir.encode("utf-8"))
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to create checkpoint")

    def rename_column_family(self, old_name: str, new_name: str) -> None:
        """
        Atomically rename a column family and its underlying directory.

        The operation waits for any in-progress flush or compaction to complete
        before renaming.

        Args:
            old_name: Current name of the column family
            new_name: New name for the column family

        Raises:
            TidesDBError: If rename fails (e.g., old_name not found, new_name exists)
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        result = _lib.tidesdb_rename_column_family(
            self._db, old_name.encode("utf-8"), new_name.encode("utf-8")
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to rename column family")

    def clone_column_family(self, source_name: str, dest_name: str) -> None:
        """
        Create a complete copy of an existing column family with a new name.

        The clone contains all the data from the source at the time of cloning.
        The clone is completely independent - modifications to one do not affect
        the other.

        Args:
            source_name: Name of the source column family to clone
            dest_name: Name for the new cloned column family

        Raises:
            TidesDBError: If clone fails (e.g., source not found, dest exists)
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        result = _lib.tidesdb_clone_column_family(
            self._db, source_name.encode("utf-8"), dest_name.encode("utf-8")
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to clone column family")

    def register_comparator(
        self,
        name: str,
        comparator_fn: callable,
        ctx: object = None,
    ) -> None:
        """
        Register a custom comparator for use with column families.

        The comparator function determines the sort order of keys throughout the
        entire system: memtables, SSTables, block indexes, and iterators.

        Built-in comparators (automatically registered):
        - "memcmp": Binary byte-by-byte comparison (default)
        - "lexicographic": Null-terminated string comparison
        - "uint64": Unsigned 64-bit integer comparison
        - "int64": Signed 64-bit integer comparison
        - "reverse": Reverse binary comparison
        - "case_insensitive": Case-insensitive ASCII comparison

        Args:
            name: Name of the comparator (used in ColumnFamilyConfig.comparator_name)
            comparator_fn: Function with signature (key1: bytes, key2: bytes) -> int
                          Returns < 0 if key1 < key2, 0 if equal, > 0 if key1 > key2
            ctx: Optional context object (not currently used, reserved for future)

        Note:
            Comparators must be registered BEFORE creating column families that use them.
            Once set, a comparator cannot be changed for a column family.
        """
        if self._closed:
            raise TidesDBError("Database is closed")

        # Wrap Python function in C-compatible callback
        def c_comparator(key1_ptr, key1_size, key2_ptr, key2_size, ctx_ptr):
            key1 = ctypes.string_at(key1_ptr, key1_size) if key1_ptr and key1_size > 0 else b""
            key2 = ctypes.string_at(key2_ptr, key2_size) if key2_ptr and key2_size > 0 else b""
            return comparator_fn(key1, key2)

        # Create C function pointer and store reference to prevent garbage collection
        c_func = COMPARATOR_FUNC(c_comparator)
        if not hasattr(self, "_comparator_refs"):
            self._comparator_refs = []
        self._comparator_refs.append(c_func)

        result = _lib.tidesdb_register_comparator(
            self._db, name.encode("utf-8"), c_func, None, None
        )
        if result != TDB_SUCCESS:
            raise TidesDBError.from_code(result, "failed to register comparator")

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
