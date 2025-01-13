#!/usr/bin/env python3
"""
TidesDB Cursor Operations Sample

This sample demonstrates the usage of cursors for iterating over data in TidesDB.
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
    Cursor,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CursorOperationsDemo:
    """Demonstrates cursor operations in TidesDB."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: Optional[TidesDB] = None
    
    def setup_database(self) -> None:
        """Initialize database and create a test column family."""
        try:
            logger.info("Opening database at: %s", self.db_path)
            self.db = TidesDB.open(self.db_path)
            
            # Create column family
            cf_name = "cursor_test_cf"
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
            
            # Insert test data
            test_data = [
                (b"key1", b"value1"),
                (b"key2", b"value2"),
                (b"key3", b"value3")
            ]
            
            for key, value in test_data:
                self.db.put(cf_name, key, value, ttl=-1)
                
        except Exception as e:
            logger.error("Failed to setup database: %s", str(e))
            self.cleanup()
            raise
    
    def demonstrate_cursor(self) -> None:
        """Demonstrate cursor operations."""
        try:
            cf_name = "cursor_test_cf"
            cursor = Cursor.init(self.db, cf_name)
            
            try:
                # Forward iteration
                logger.info("Forward iteration:")
                while True:
                    try:
                        key, value = cursor.get()
                        logger.info("Key: %s, Value: %s", key, value)
                        cursor.next()
                    except Exception:
                        break
            finally:
                cursor.free()
            
            logger.info("Cursor operations completed successfully!")
            
        except Exception as e:
            logger.error("Error during cursor operations: %s", str(e))
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
    """Main entry point for the cursor operations demo."""
    db_path = str(Path.cwd() / "cursor_output")
    
    demo = CursorOperationsDemo(db_path)
    try:
        demo.setup_database()
        demo.demonstrate_cursor()
    except Exception as e:
        logger.error("Demo failed: %s", str(e))
        sys.exit(1)
    finally:
        demo.cleanup()

if __name__ == "__main__":
    main()