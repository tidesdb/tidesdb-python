#!/usr/bin/env python3
"""
Test suite for TidesDB transaction operations.
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
    Transaction,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestTransactionOperations(unittest.TestCase):
    def setUp(self):
        """Set up test database."""
        logger.info("Setting up test environment for transaction operations")
        self.test_dir = tempfile.mkdtemp()
        logger.info("Created temporary directory: %s", self.test_dir)
        self.db = None
        self.cf_name = "transaction_test_cf"
        
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
    
    def test_successful_transaction(self):
        """Test successful transaction completion."""
        logger.info("Testing successful transaction completion")
        txn = None
        try:
            logger.info("Beginning new transaction")
            txn = Transaction.begin(self.db, self.cf_name)
            
            test_data = [
                (b"txn:1", b"Transaction Value 1"),
                (b"txn:2", b"Transaction Value 2")
            ]
            
            # Add items in transaction
            logger.info("Adding items in transaction:")
            for key, value in test_data:
                logger.info("Adding key-value: %s -> %s", key, value)
                txn.put(key, value, ttl=-1)
            
            # Commit transaction
            logger.info("Committing transaction")
            txn.commit()
            
            # Verify items were added
            logger.info("Verifying committed data:")
            for key, value in test_data:
                logger.info("Checking key: %s", key)
                retrieved = self.db.get(self.cf_name, key)
                logger.info("Retrieved value: %s", retrieved)
                self.assertEqual(value, retrieved)
                
            logger.info("Successful transaction test completed")
                
        finally:
            if txn:
                logger.info("Freeing transaction")
                txn.free()
    
    def test_transaction_rollback(self):
        """Test transaction rollback."""
        logger.info("Testing transaction rollback")
        txn = None
        try:
            logger.info("Beginning new transaction")
            txn = Transaction.begin(self.db, self.cf_name)
            
            # Add item in transaction
            key, value = b"rollback:1", b"Rollback Value"
            logger.info("Adding test key-value: %s -> %s", key, value)
            txn.put(key, value, ttl=-1)
            
            # Rollback transaction
            logger.info("Rolling back transaction")
            txn.rollback()
            
            # Verify item was not added
            logger.info("Verifying data was not committed")
            with self.assertRaises(Exception):
                self.db.get(self.cf_name, key)
            logger.info("Rollback test completed successfully")
                
        finally:
            if txn:
                logger.info("Freeing transaction")
                txn.free()
    
    def test_transaction_isolation(self):
        """Test transaction isolation."""
        logger.info("Testing transaction isolation")
        txn = None
        try:
            logger.info("Beginning new transaction")
            txn = Transaction.begin(self.db, self.cf_name)
            
            key, value = b"isolation:1", b"Isolation Value"
            original_value = b"Original Value"
            
            # Add item outside transaction
            logger.info("Adding original value outside transaction: %s -> %s", 
                       key, original_value)
            self.db.put(self.cf_name, key, original_value, ttl=-1)
            
            # Modify in transaction
            logger.info("Modifying value in transaction: %s -> %s", key, value)
            txn.put(key, value, ttl=-1)
            
            # Before commit, should see original value
            logger.info("Verifying isolation before commit")
            retrieved = self.db.get(self.cf_name, key)
            logger.info("Retrieved value (should be original): %s", retrieved)
            self.assertEqual(retrieved, original_value)
            
            # After commit, should see new value
            logger.info("Committing transaction")
            txn.commit()
            
            logger.info("Verifying final value after commit")
            retrieved = self.db.get(self.cf_name, key)
            logger.info("Retrieved value (should be new): %s", retrieved)
            self.assertEqual(retrieved, value)
            
            logger.info("Transaction isolation test completed successfully")
            
        finally:
            if txn:
                logger.info("Freeing transaction")
                txn.free()

if __name__ == '__main__':
    unittest.main()