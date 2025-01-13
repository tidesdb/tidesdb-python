#!/usr/bin/env python3
"""
TidesDB Basic Operations Sample

This sample demonstrates the fundamental operations of TidesDB including:
- Database initialization
- Column family management
- Basic key-value operations (put/get/delete)
- Resource cleanup and error handling

For more advanced usage, see the tutorials directory.
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
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BasicOperationsDemo:
    """Demonstrates basic TidesDB operations with proper error handling."""
    
    def __init__(self, db_path: str):
        """
        Initialize the demo with a database path.
        
        Args:
            db_path: Path where the database will be created
        """
        self.db_path = db_path
        self.db: Optional[TidesDB] = None
    
    def setup_database(self) -> None:
        """Initialize database and create necessary column families."""
        try:
            logger.info("Opening database at: %s", self.db_path)
            self.db = TidesDB.open(self.db_path)
            
            # Default column family configuration
            cf_config = {
                "name": "default_cf",
                "flush_threshold": 64 * 1024 * 1024,  # 64MB
                "max_level": 12,
                "probability": 0.24,
                "compression": True,
                "compression_algo": TidesDBCompressionAlgo.COMPRESS_SNAPPY,
                "bloom_filter": True,
                "memtable_ds": TidesDBMemtableDS.SKIP_LIST
            }
            
            logger.info("Creating column family: %s", cf_config["name"])
            self.db.create_column_family(
                cf_config["name"],
                cf_config["flush_threshold"],
                cf_config["max_level"],
                cf_config["probability"],
                cf_config["compression"],
                cf_config["compression_algo"],
                cf_config["bloom_filter"],
                cf_config["memtable_ds"]
            )
            
        except Exception as e:
            logger.error("Failed to setup database: %s", str(e))
            self.cleanup()
            raise
    
    def demonstrate_operations(self) -> None:
        """Demonstrate basic key-value operations."""
        try:
            cf_name = "default_cf"
            
            # Basic Put operation
            key, value = b"user:1", b"John Doe"
            logger.info("Putting key-value: %s -> %s", key, value)
            self.db.put(cf_name, key, value, ttl=-1)
            
            # Basic Get operation
            retrieved = self.db.get(cf_name, key)
            logger.info("Retrieved value: %s", retrieved)
            assert retrieved == value, f"Value mismatch: expected {value}, got {retrieved}"
            
            # Delete operation
            logger.info("Deleting key: %s", key)
            self.db.delete(cf_name, key)
            
            # Verify deletion
            try:
                _ = self.db.get(cf_name, key)
                raise AssertionError("Key should have been deleted")
            except Exception as e:
                logger.info("Key successfully deleted: %s", str(e))
            
            logger.info("All basic operations completed successfully!")
            
        except Exception as e:
            logger.error("Error during operations: %s", str(e))
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
    """Main entry point for the basic operations demo."""
    db_path = str(Path.cwd() / "output")
    
    demo = BasicOperationsDemo(db_path)
    try:
        demo.setup_database()
        demo.demonstrate_operations()
    except Exception as e:
        logger.error("Demo failed: %s", str(e))
        sys.exit(1)
    finally:
        demo.cleanup()

if __name__ == "__main__":
    main()