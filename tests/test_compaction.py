#!/usr/bin/env python3
"""
Test suite for TidesDB background compaction.
"""
import unittest
import time
import tempfile
import shutil
import logging
from pathlib import Path
from tidesdb import (
    TidesDB,
    TidesDBCompressionAlgo,
    TidesDBMemtableDS,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestBackgroundCompaction(unittest.TestCase):
    def setUp(self):
        """Set up test database for background compaction."""
        logger.info("Setting up test environment for compaction")
        self.test_dir = tempfile.mkdtemp()
        logger.info("Created temporary directory: %s", self.test_dir)
        self.db = None
        self.cf_name = "compaction_test_cf"
        
        try:
            logger.info("Opening database")
            self.db = TidesDB.open(self.test_dir)
            
            logger.info("Creating column family: %s", self.cf_name)
            
            self.db.create_column_family(
                self.cf_name,                         
                64 * 1024 * 1024,                     
                12,                                   
                0.24,                                
                True,                                
                TidesDBCompressionAlgo.COMPRESS_SNAPPY,
                True,                        
                TidesDBMemtableDS.SKIP_LIST       
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

    def test_background_compaction(self):
        """Test the background compaction process."""
        logger.info("Starting background compaction test")

        logger.info("Inserting data to trigger compaction")
        batch_size = 500
        total_batches = 5 
        
        for batch in range(total_batches):
            for i in range(batch_size):
                key = f"key:{batch * batch_size + i:08d}".encode()
                value = f"value:{batch * batch_size + i:08d}".encode() * 2000
                self.db.put(self.cf_name, key, value, ttl=-1)
        
        logger.info(f"Inserted {batch_size * total_batches} records")

        logger.info("Starting background compaction")
        self.db.start_background_compaction(
            self.cf_name,
            interval_seconds=1,
            min_sstables=2
        )

        logger.info("Monitoring compaction progress...")
        start_time = time.time()
        while time.time() - start_time < 30:
            time.sleep(1)
            analysis = self.analyze_sstables()
            logger.info(f"SSTable count: {analysis['total_count']}")
            if analysis['total_count'] <= 5:
                logger.info("Compaction target achieved.")
                break
        
        self.assertTrue(analysis['total_count'] <= 5, "Compaction failed to reduce SSTables to the target.")
        logger.info("Background compaction test completed successfully")

    def analyze_sstables(self):
        """Analyze the SSTable files in the database."""
        cf_path = Path(self.test_dir) / self.cf_name
        sst_files = list(cf_path.glob("sstable_*.sst"))
        analysis = {
            'total_count': len(sst_files),
            'size_distribution': {}
        }
        
        for sst in sst_files:
            size = sst.stat().st_size
            analysis['size_distribution'][size // (1024 * 1024)] = analysis['size_distribution'].get(size // (1024 * 1024), 0) + 1
            
        return analysis

if __name__ == '__main__':
    unittest.main()
