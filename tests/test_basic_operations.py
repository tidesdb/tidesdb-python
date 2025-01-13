#!/usr/bin/env python3
"""
Test suite for TidesDB basic operations.
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
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestBasicOperations(unittest.TestCase):
    def setUp(self):
        """Set up test database."""
        logger.info("Setting up test environment")
        self.test_dir = tempfile.mkdtemp()
        logger.info("Created temporary directory: %s", self.test_dir)
        self.db = None
        self.cf_name = "test_cf"
        
        try:
            logger.info("Opening database")
            self.db = TidesDB.open(self.test_dir)
            
            logger.info("Creating column family: %s", self.cf_name)
            # Create column family with parameters in correct order
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
    
    def test_put_and_get(self):
        """Test basic put and get operations."""
        logger.info("Testing put and get operations")
        key, value = b"test:1", b"Test Value"
        
        logger.info("Putting key-value: %s -> %s", key, value)
        self.db.put(self.cf_name, key, value, ttl=-1)
        
        logger.info("Getting value for key: %s", key)
        retrieved = self.db.get(self.cf_name, key)
        logger.info("Retrieved value: %s", retrieved)
        
        self.assertEqual(value, retrieved)
        logger.info("Put/Get test completed successfully")
    
    def test_delete(self):
        """Test delete operation."""
        logger.info("Testing delete operation")
        key, value = b"test:2", b"Delete Test"
        
        logger.info("Putting test key-value: %s -> %s", key, value)
        self.db.put(self.cf_name, key, value, ttl=-1)
        
        logger.info("Deleting key: %s", key)
        self.db.delete(self.cf_name, key)
        
        logger.info("Verifying key deletion")
        with self.assertRaises(Exception):
            self.db.get(self.cf_name, key)
        logger.info("Delete test completed successfully")
    
    def test_multiple_operations(self):
        """Test multiple operations in sequence."""
        logger.info("Testing multiple operations")
        test_data = [
            (b"multi:1", b"Value 1"),
            (b"multi:2", b"Value 2"),
            (b"multi:3", b"Value 3")
        ]
        
        # Put all values
        logger.info("Inserting multiple values")
        for key, value in test_data:
            logger.info("Putting key-value: %s -> %s", key, value)
            self.db.put(self.cf_name, key, value, ttl=-1)
        
        # Verify all values
        logger.info("Verifying inserted values")
        for key, value in test_data:
            logger.info("Getting value for key: %s", key)
            retrieved = self.db.get(self.cf_name, key)
            logger.info("Retrieved value: %s", retrieved)
            self.assertEqual(value, retrieved)
        
        # Delete and verify deletion
        logger.info("Testing multiple deletions")
        for key, _ in test_data:
            logger.info("Deleting key: %s", key)
            self.db.delete(self.cf_name, key)
            with self.assertRaises(Exception):
                self.db.get(self.cf_name, key)
                
        logger.info("Multiple operations test completed successfully")

if __name__ == '__main__':
    unittest.main()