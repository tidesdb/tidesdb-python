"""
TidesDB Python Bindings

Official Python bindings for TidesDB v1+.
"""

from .tidesdb import (
    TidesDB,
    Transaction,
    Iterator,
    ColumnFamily,
    ColumnFamilyConfig,
    ColumnFamilyStat,
    CompressionAlgo,
    SyncMode,
    ErrorCode,
    TidesDBException,
)

__version__ = "1.0.0"
__author__ = "TidesDB Authors"
__license__ = "MPL-2.0"

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