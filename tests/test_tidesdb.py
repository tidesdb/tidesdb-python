"""
TidesDB Python Bindings Tests

Copyright (C) TidesDB
"""

import unittest
import os
import shutil
import time
import pickle
from pathlib import Path
from tidesdb import (
    TidesDB,
    ColumnFamilyConfig,
    CompressionAlgo,
    SyncMode,
    TidesDBException,
    ErrorCode
)


class TestTidesDB(unittest.TestCase):
    """Test suite for TidesDB Python bindings."""
    
    def setUp(self):
        """Set up test database."""
        # Use absolute path to avoid encoding issues
        self.test_db_path = os.path.abspath("test_db")
        if os.path.exists(self.test_db_path):
            shutil.rmtree(self.test_db_path)
    
    def tearDown(self):
        """Clean up test database."""
        if os.path.exists(self.test_db_path):
            shutil.rmtree(self.test_db_path)
    
    def test_open_close(self):
        """Test opening and closing database."""
        db = TidesDB(self.test_db_path)
        self.assertIsNotNone(db)
        db.close()
    
    def test_context_manager(self):
        """Test database as context manager."""
        with TidesDB(self.test_db_path) as db:
            self.assertIsNotNone(db)
        # Database should be closed after context
    
    def test_create_drop_column_family(self):
        """Test creating and dropping column families."""
        with TidesDB(self.test_db_path) as db:
            # Create with default config
            db.create_column_family("test_cf")
            
            # Verify it exists
            cf_list = db.list_column_families()
            self.assertIn("test_cf", cf_list)
            
            # Drop it
            db.drop_column_family("test_cf")
            
            # Verify it's gone
            cf_list = db.list_column_families()
            self.assertNotIn("test_cf", cf_list)
    
    def test_create_column_family_with_config(self):
        """Test creating column family with custom configuration."""
        with TidesDB(self.test_db_path) as db:
            config = ColumnFamilyConfig(
                memtable_flush_size=128 * 1024 * 1024,  # 128MB
                max_sstables_before_compaction=512,
                compaction_threads=4,
                compressed=True,
                compress_algo=CompressionAlgo.LZ4,
                bloom_filter_fp_rate=0.01,
                enable_background_compaction=True,
                sync_mode=SyncMode.BACKGROUND,
                sync_interval=1000
            )
            
            db.create_column_family("custom_cf", config)
            
            # Verify configuration
            stats = db.get_column_family_stats("custom_cf")
            self.assertEqual(stats.config.memtable_flush_size, 128 * 1024 * 1024)
            self.assertEqual(stats.config.compress_algo, CompressionAlgo.LZ4)
            self.assertTrue(stats.config.compressed)
            self.assertTrue(stats.config.enable_background_compaction)
            self.assertEqual(stats.config.sync_mode, SyncMode.BACKGROUND)
    
    def test_list_column_families(self):
        """Test listing column families."""
        with TidesDB(self.test_db_path) as db:
            # Create multiple column families
            cf_names = ["cf1", "cf2", "cf3"]
            for name in cf_names:
                db.create_column_family(name)
            
            # List them
            cf_list = db.list_column_families()
            # Check that all created column families are present
            # (there may be additional ones from previous test runs)
            for name in cf_names:
                self.assertIn(name, cf_list)
    
    def test_transaction_put_get_delete(self):
        """Test basic CRUD operations with transactions."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Put data
            with db.begin_txn() as txn:
                txn.put("test_cf", b"key1", b"value1")
                txn.put("test_cf", b"key2", b"value2")
                txn.commit()
            
            # Get data
            with db.begin_read_txn() as txn:
                value1 = txn.get("test_cf", b"key1")
                self.assertEqual(value1, b"value1")
                
                value2 = txn.get("test_cf", b"key2")
                self.assertEqual(value2, b"value2")
            
            # Delete data
            with db.begin_txn() as txn:
                txn.delete("test_cf", b"key1")
                txn.commit()
            
            # Verify deletion
            with db.begin_read_txn() as txn:
                with self.assertRaises(TidesDBException):
                    txn.get("test_cf", b"key1")
    
    def test_transaction_with_ttl(self):
        """Test transactions with TTL."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Put with TTL (2 seconds from now)
            ttl = int(time.time()) + 2
            with db.begin_txn() as txn:
                txn.put("test_cf", b"temp_key", b"temp_value", ttl)
                txn.commit()
            
            # Verify it exists
            with db.begin_read_txn() as txn:
                value = txn.get("test_cf", b"temp_key")
                self.assertEqual(value, b"temp_value")
            
            # Wait for expiration
            time.sleep(3)
            
            # Verify it's expired
            with db.begin_read_txn() as txn:
                with self.assertRaises(TidesDBException):
                    txn.get("test_cf", b"temp_key")
    
    def test_multi_operation_transaction(self):
        """Test transaction with multiple operations."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Multiple operations in one transaction
            with db.begin_txn() as txn:
                for i in range(10):
                    key = f"key{i}".encode()
                    value = f"value{i}".encode()
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Verify all were written
            with db.begin_read_txn() as txn:
                for i in range(10):
                    key = f"key{i}".encode()
                    expected_value = f"value{i}".encode()
                    value = txn.get("test_cf", key)
                    self.assertEqual(value, expected_value)
    
    def test_transaction_rollback(self):
        """Test transaction rollback."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Put some data and rollback
            with db.begin_txn() as txn:
                txn.put("test_cf", b"rollback_key", b"rollback_value")
                txn.rollback()
            
            # Verify data wasn't written
            with db.begin_read_txn() as txn:
                with self.assertRaises(TidesDBException):
                    txn.get("test_cf", b"rollback_key")
    
    def test_transaction_auto_rollback_on_exception(self):
        """Test transaction automatically rolls back on exception."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Exception in context manager should trigger rollback
            try:
                with db.begin_txn() as txn:
                    txn.put("test_cf", b"error_key", b"error_value")
                    raise ValueError("Test error")
            except ValueError:
                pass
            
            # Verify data wasn't written
            with db.begin_read_txn() as txn:
                with self.assertRaises(TidesDBException):
                    txn.get("test_cf", b"error_key")
    
    def test_iterator_forward(self):
        """Test forward iteration."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Insert test data
            test_data = {
                b"key1": b"value1",
                b"key2": b"value2",
                b"key3": b"value3",
                b"key4": b"value4",
                b"key5": b"value5",
            }
            
            with db.begin_txn() as txn:
                for key, value in test_data.items():
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Iterate forward
            with db.begin_read_txn() as txn:
                with txn.new_iterator("test_cf") as it:
                    it.seek_to_first()
                    
                    count = 0
                    while it.valid():
                        key = it.key()
                        value = it.value()
                        
                        self.assertIn(key, test_data)
                        self.assertEqual(value, test_data[key])
                        
                        count += 1
                        it.next()
                    
                    self.assertEqual(count, len(test_data))
    
    def test_iterator_backward(self):
        """Test backward iteration."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Insert test data
            test_data = {
                b"key1": b"value1",
                b"key2": b"value2",
                b"key3": b"value3",
            }
            
            with db.begin_txn() as txn:
                for key, value in test_data.items():
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Iterate backward
            with db.begin_read_txn() as txn:
                with txn.new_iterator("test_cf") as it:
                    it.seek_to_last()
                    
                    count = 0
                    while it.valid():
                        key = it.key()
                        value = it.value()
                        
                        self.assertIn(key, test_data)
                        self.assertEqual(value, test_data[key])
                        
                        count += 1
                        it.prev()
                    
                    self.assertEqual(count, len(test_data))
    
    def test_iterator_as_python_iterator(self):
        """Test iterator as Python iterator."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Insert test data
            test_data = {
                b"key1": b"value1",
                b"key2": b"value2",
                b"key3": b"value3",
            }
            
            with db.begin_txn() as txn:
                for key, value in test_data.items():
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Use as Python iterator
            with db.begin_read_txn() as txn:
                with txn.new_iterator("test_cf") as it:
                    it.seek_to_first()
                    
                    results = list(it)
                    self.assertEqual(len(results), len(test_data))
                    
                    for key, value in results:
                        self.assertIn(key, test_data)
                        self.assertEqual(value, test_data[key])
    
    def test_get_column_family_stats(self):
        """Test getting column family statistics."""
        with TidesDB(self.test_db_path) as db:
            config = ColumnFamilyConfig(
                memtable_flush_size=2 * 1024 * 1024,  # 2MB
                max_level=12,
                compressed=True,
                compress_algo=CompressionAlgo.SNAPPY,
                bloom_filter_fp_rate=0.01
            )
            
            db.create_column_family("test_cf", config)
            
            # Add some data
            with db.begin_txn() as txn:
                for i in range(10):
                    key = f"key{i}".encode()
                    value = f"value{i}".encode()
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Get statistics
            stats = db.get_column_family_stats("test_cf")
            
            self.assertEqual(stats.name, "test_cf")
            self.assertEqual(stats.config.memtable_flush_size, 2 * 1024 * 1024)
            self.assertEqual(stats.config.max_level, 12)
            self.assertTrue(stats.config.compressed)
            self.assertEqual(stats.config.compress_algo, CompressionAlgo.SNAPPY)
            # Data may be in memtable or flushed to SSTables
            self.assertGreaterEqual(stats.memtable_entries, 0)
            self.assertGreaterEqual(stats.memtable_size, 0)
    
    def test_compaction(self):
        """Test manual compaction."""
        with TidesDB(self.test_db_path) as db:
            # Create CF with small flush threshold to force SSTables
            config = ColumnFamilyConfig(
                memtable_flush_size=1024,  # 1KB
                enable_background_compaction=False,
                compaction_threads=2
            )
            
            db.create_column_family("test_cf", config)
            
            # Add data to create multiple SSTables
            for batch in range(5):
                with db.begin_txn() as txn:
                    for i in range(20):
                        key = f"key{batch}_{i}".encode()
                        value = b"x" * 512  # 512 bytes
                        txn.put("test_cf", key, value)
                    txn.commit()
            
            # Get column family for compaction
            cf = db.get_column_family("test_cf")
            
            # Check stats before compaction
            stats_before = db.get_column_family_stats("test_cf")
            
            # Perform compaction if we have enough SSTables
            if stats_before.num_sstables >= 2:
                cf.compact()
                
                stats_after = db.get_column_family_stats("test_cf")
                # Note: compaction may or may not reduce SSTable count
                # depending on timing, but it should complete without error
                self.assertIsNotNone(stats_after)
    
    def test_sync_modes(self):
        """Test different sync modes."""
        sync_modes = [
            (SyncMode.NONE, "none"),
            (SyncMode.BACKGROUND, "background"),
            (SyncMode.FULL, "full"),
        ]
        
        with TidesDB(self.test_db_path) as db:
            for mode, name in sync_modes:
                cf_name = f"cf_{name}"
                
                config = ColumnFamilyConfig(
                    sync_mode=mode,
                    sync_interval=1000 if mode == SyncMode.BACKGROUND else 0
                )
                
                db.create_column_family(cf_name, config)
                
                # Verify sync mode
                stats = db.get_column_family_stats(cf_name)
                self.assertEqual(stats.config.sync_mode, mode)
    
    def test_compression_algorithms(self):
        """Test different compression algorithms."""
        # Test with compression disabled
        with TidesDB(self.test_db_path) as db:
            config = ColumnFamilyConfig(compressed=False)
            db.create_column_family("cf_none", config)
            stats = db.get_column_family_stats("cf_none")
            self.assertFalse(stats.config.compressed)
        
        # Test with different compression algorithms
        algorithms = [
            (CompressionAlgo.SNAPPY, "snappy"),
            (CompressionAlgo.LZ4, "lz4"),
            (CompressionAlgo.ZSTD, "zstd"),
        ]
        
        with TidesDB(self.test_db_path) as db:
            for algo, name in algorithms:
                cf_name = f"cf_{name}"
                
                config = ColumnFamilyConfig(
                    compressed=True,
                    compress_algo=algo
                )
                
                db.create_column_family(cf_name, config)
                
                # Verify compression
                stats = db.get_column_family_stats(cf_name)
                self.assertTrue(stats.config.compressed)
                self.assertEqual(stats.config.compress_algo, algo)
    
    def test_pickle_support(self):
        """Test storing Python objects with pickle."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Store complex Python object
            test_obj = {
                "name": "John Doe",
                "age": 30,
                "scores": [95, 87, 92],
                "metadata": {"city": "NYC", "country": "USA"}
            }
            
            key = b"user:123"
            value = pickle.dumps(test_obj)
            
            with db.begin_txn() as txn:
                txn.put("test_cf", key, value)
                txn.commit()
            
            # Retrieve and deserialize
            with db.begin_read_txn() as txn:
                stored_value = txn.get("test_cf", key)
                retrieved_obj = pickle.loads(stored_value)
                
                self.assertEqual(retrieved_obj, test_obj)
    
    def test_error_handling(self):
        """Test error handling."""
        with TidesDB(self.test_db_path) as db:
            # Try to get stats for non-existent CF
            with self.assertRaises(TidesDBException) as ctx:
                db.get_column_family_stats("nonexistent_cf")
            self.assertIsInstance(ctx.exception.code, int)
            
            # Try to drop non-existent CF
            with self.assertRaises(TidesDBException):
                db.drop_column_family("nonexistent_cf")
            
            # Try to get from non-existent CF
            db.create_column_family("test_cf")
            with db.begin_read_txn() as txn:
                with self.assertRaises(TidesDBException):
                    txn.get("nonexistent_cf", b"key")
    
    def test_large_values(self):
        """Test storing large values."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            # Store 1MB value
            large_value = b"x" * (1024 * 1024)
            
            with db.begin_txn() as txn:
                txn.put("test_cf", b"large_key", large_value)
                txn.commit()
            
            # Retrieve it
            with db.begin_read_txn() as txn:
                retrieved = txn.get("test_cf", b"large_key")
                self.assertEqual(len(retrieved), len(large_value))
                self.assertEqual(retrieved, large_value)
    
    def test_many_keys(self):
        """Test storing many keys."""
        with TidesDB(self.test_db_path) as db:
            db.create_column_family("test_cf")
            
            num_keys = 1000
            
            # Insert many keys
            with db.begin_txn() as txn:
                for i in range(num_keys):
                    key = f"key_{i:06d}".encode()
                    value = f"value_{i}".encode()
                    txn.put("test_cf", key, value)
                txn.commit()
            
            # Verify count with iterator
            with db.begin_read_txn() as txn:
                with txn.new_iterator("test_cf") as it:
                    it.seek_to_first()
                    count = sum(1 for _ in it)
                    self.assertEqual(count, num_keys)


def run_tests():
    """Run all tests."""
    unittest.main()


if __name__ == '__main__':
    run_tests()