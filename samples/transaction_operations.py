#!/usr/bin/env python3
"""
TidesDB Transaction Operations Sample

This sample demonstrates the usage of transactions in TidesDB.
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from pathlib import Path
from typing import Optional

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

class TransactionOperationsDemo:
    """Demonstrates transaction operations in TidesDB."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: Optional[TidesDB] = None
    
    def setup_database(self) -> None:
        """Initialize database and create a test column family."""
        try:
            logger.info("Opening database at: %s", self.db_path)
            self.db = TidesDB.open(self.db_path)
            
            # Create column family
            cf_name = "transaction_test_cf"
            logger.info("Creating column family: %s", cf_name)
            self.db.create_column_family(
                cf_name,
                flush_threshold=64 * 1024 * 1024,  # 64MB
                max_level=12,
                probability=0.24,
                compressed=True,
                compress_algo=TidesDBCompressionAlgo.COMPRESS_SNAPPY,
                bloom_filter=True,
                memtable_ds=TidesDBMemtableDS.SKIP_LIST
            )
            
        except Exception as e:
            logger.error("Failed to setup database: %s", str(e))
            self.cleanup()
            raise
    
    def demonstrate_transaction(self) -> None:
        """Demonstrate transaction operations."""
        try:
            cf_name = "transaction_test_cf"
            
            # Start a transaction
            logger.info("Starting transaction")
            txn = Transaction.begin(self.db, cf_name)
            
            try:
                # Perform multiple operations
                txn.put(b"user:1", b"Alice", ttl=-1)
                txn.put(b"user:2", b"Bob", ttl=-1)
                
                # Commit the transaction
                logger.info("Committing transaction")
                txn.commit()
                
                # Verify the changes
                logger.info("Verifying changes:")
                for key in [b"user:1", b"user:2"]:
                    value = self.db.get(cf_name, key)
                    logger.info("Key: %s, Value: %s", key, value)
                    
            finally:
                txn.free()
            
            logger.info("Transaction operations completed successfully!")
            
        except Exception as e:
            logger.error("Error during transaction operations: %s", str(e))
            raise
    
    def cleanup(self) -> None:
        """Clean up resources."""
        if self.db:
            try:
                logger.info("Closing database")
                self.db.close()
            except Exception as e:
                logger.error("Error closing database: %s", str(e))

def main():
    """Main entry point for the transaction operations demo."""
    db_path = str(Path.cwd() / "transaction_output")
    
    demo = TransactionOperationsDemo(db_path)
    try:
        demo.setup_database()
        demo.demonstrate_transaction()
    except Exception as e:
        logger.error("Demo failed: %s", str(e))
        sys.exit(1)
    finally:
        demo.cleanup()

if __name__ == "__main__":
    main()