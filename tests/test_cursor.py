#!/usr/bin/env python3
"""
Test suite for TidesDB cursor operations.
"""
import unittest
import tempfile
import shutil
import logging
from pathlib import Path

from tidesdb import (
    TidesDB,
    TidesDBCompressionAlgo,
    TidesDBMemtableDS,
    Cursor,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestCursorOperations(unittest.TestCase):
    def setUp(self):
        """Set up test database with sample data."""
        logger.info("Setting up test environment for cursor operations")
        self.test_dir = tempfile.mkdtemp()
        logger.info("Created temporary directory: %s", self.test_dir)
        self.db = None
        self.cf_name = "cursor_test_cf"
        self.test_data = [
            (b"key:1", b"Value 1"),
            (b"key:2", b"Value 2"),
            (b"key:3", b"Value 3"),
            (b"key:4", b"Value 4")
        ]
        
        try:
            logger.info("Opening database")
            self.db = TidesDB.open(self.test_dir)
            
            logger.info("Creating column family: %s", self.cf_name)
            self.db.create_column_family(
                self.cf_name,                          # name
                64 * 1024 * 1024,                      # flush_threshold (64MB)
                12,                                    # max_level
                0.24,                                  # probability
                True,                                  # compressed
                TidesDBCompressionAlgo.COMPRESS_SNAPPY,# compress_algo
                True,                                  # bloom_filter
                TidesDBMemtableDS.SKIP_LIST           # memtable_ds
            )
            
            # Insert test data
            logger.info("Inserting test data:")
            for key, value in self.test_data:
                logger.info("Inserting key-value: %s -> %s", key, value)
                self.db.put(self.cf_name, key, value, ttl=-1)
                
            logger.info("Test environment setup completed successfully")
                
        except Exception as e:
            logger.error("Failed to setup test environment: %s", str(e))
            self.tearDown()
            raise
    
    def tearDown(self):
        """Clean up test resources."""
        logger.info("Cleaning up test resources")
        if self.db:
            try:
                logger.info("Closing database")
                self.db.close()
            except Exception as e:
                logger.error("Error closing database: %s", str(e))
        
        logger.info("Removing temporary directory: %s", self.test_dir)
        shutil.rmtree(self.test_dir, ignore_errors=True)
        logger.info("Cleanup completed")
    
    def test_forward_iteration(self):
        """Test cursor forward iteration."""
        logger.info("Testing cursor forward iteration")
        cursor = None
        try:
            logger.info("Initializing cursor for column family: %s", self.cf_name)
            cursor = Cursor.init(self.db, self.cf_name)
            
            retrieved_data = []
            logger.info("Starting forward iteration")
            while True:
                try:
                    key, value = cursor.get()
                    logger.info("Retrieved key-value: %s -> %s", key, value)
                    retrieved_data.append((key, value))
                    cursor.next()
                except Exception:
                    logger.info("Reached end of data")
                    break
                    
            logger.info("Verifying retrieved data")
            self.assertEqual(len(retrieved_data), len(self.test_data))
            for i, (key, value) in enumerate(retrieved_data):
                logger.info("Verifying item %d: %s -> %s", i, key, value)
                self.assertEqual(key, self.test_data[i][0])
                self.assertEqual(value, self.test_data[i][1])
                
            logger.info("Forward iteration test completed successfully")
                
        finally:
            if cursor:
                logger.info("Freeing cursor")
                cursor.free()
    
    def test_cursor_after_modifications(self):
        """Test cursor behavior after database modifications."""
        logger.info("Testing cursor behavior after modifications")
        
        # Add a new item
        new_key, new_value = b"key:5", b"Value 5"
        logger.info("Adding new item: %s -> %s", new_key, new_value)
        self.db.put(self.cf_name, new_key, new_value, ttl=-1)
        
        cursor = None
        try:
            logger.info("Initializing cursor for modified data")
            cursor = Cursor.init(self.db, self.cf_name)
            
            # Count items through cursor
            count = 0
            logger.info("Counting items through cursor")
            while True:
                try:
                    key, value = cursor.get()
                    logger.info("Found item: %s -> %s", key, value)
                    count += 1
                    cursor.next()
                except Exception:
                    logger.info("Reached end of data")
                    break
            
            logger.info("Verifying item count. Expected: %d, Got: %d", 
                    len(self.test_data) + 1, count)
            self.assertEqual(count, len(self.test_data) + 1)
            logger.info("Cursor after modifications test completed successfully")
            
        finally:
            if cursor:
                logger.info("Freeing cursor")
                cursor.free()

if __name__ == '__main__':
    unittest.main()