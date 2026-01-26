"""
TidesDB Python Bindings

Official Python bindings for TidesDB v7+ - A high-performance embedded key-value storage engine.

Copyright (C) TidesDB
Licensed under the Mozilla Public License, v. 2.0
"""

from .tidesdb import (
    TidesDB,
    Transaction,
    Iterator,
    ColumnFamily,
    Config,
    ColumnFamilyConfig,
    Stats,
    CacheStats,
    CompressionAlgorithm,
    SyncMode,
    LogLevel,
    IsolationLevel,
    TidesDBError,
    default_config,
    default_column_family_config,
    save_config_to_ini,
    COMPARATOR_FUNC,
)

__version__ = "7.3.1"
__all__ = [
    "TidesDB",
    "Transaction",
    "Iterator",
    "ColumnFamily",
    "Config",
    "ColumnFamilyConfig",
    "Stats",
    "CacheStats",
    "CompressionAlgorithm",
    "SyncMode",
    "LogLevel",
    "IsolationLevel",
    "TidesDBError",
    "default_config",
    "default_column_family_config",
    "save_config_to_ini",
    "COMPARATOR_FUNC",
]
