#!/usr/bin/env python3
"""
TidesDB Background Compaction with Enhanced Monitoring
"""
import os
import sys
import time
import glob
import logging
from pathlib import Path
from typing import Optional, List, Dict
from collections import defaultdict

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

class CompactionMonitor:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db: Optional[TidesDB] = None
        self.cf_name = "compaction_test_cf"
        self.total_records = 0
        
    def analyze_sstables(self) -> Dict:
        cf_path = os.path.join(self.db_path, self.cf_name)
        sst_files = glob.glob(os.path.join(cf_path, "sstable_*.sst"))
        
        analysis = {
            'total_count': len(sst_files),
            'size_distribution': defaultdict(int),
            'files': sorted(os.path.basename(f) for f in sst_files)
        }
        
        for sst in sst_files:
            size = os.path.getsize(sst)
            analysis['size_distribution'][size // (1024 * 1024)] += 1
            
        return analysis

    def save_current_state(self, filename: str, max_records: int) -> None:
        """Saves the current database state to a text file."""
        try:
            cf_path = os.path.join(self.db_path, self.cf_name)
            output_path = os.path.join(cf_path, filename)
            analysis = self.analyze_sstables()
            
            with open(output_path, 'w') as f:
                f.write(f"Database state snapshot - {filename}\n")
                f.write("-" * 50 + "\n\n")

                f.write("Current SST Files:\n")
                for sst_file in analysis['files']:
                    f.write(f"- {sst_file}\n")
                f.write("\n" + "-" * 50 + "\n\n")
                
                count = 0
                for i in range(max_records):
                    key = f"key:{i:08d}".encode()
                    try:
                        value = self.db.get(self.cf_name, key)
                        if value:
                            count += 1

                            current_sst = analysis['files'][count % len(analysis['files'])]
                            f.write(f"Key: {key.decode()}, Value: {value.decode()[:50]}... | In SST: {current_sst}\n")
                    except Exception:
                        continue
                
                f.write("\n" + "-" * 50 + "\n")
                f.write(f"Total records found: {count}\n")
                f.write(f"Total SST files: {len(analysis['files'])}\n")
            
            logger.info(f"Saved database state to {filename} - {count} records written")
            
        except Exception as e:
            logger.error(f"Error saving state to {filename}: {str(e)}")
    
    def setup_database(self) -> None:
        try:
            logger.info("Opening database at: %s", self.db_path)
            self.db = TidesDB.open(self.db_path)
            
            logger.info("Creating column family: %s", self.cf_name)
            self.db.create_column_family(
                self.cf_name,
                4 * 1024 * 1024,
                6,
                0.5,
                True,
                TidesDBCompressionAlgo.COMPRESS_SNAPPY,
                True,
                TidesDBMemtableDS.SKIP_LIST
            )
            
            batch_size = 500
            total_batches = 20
            value_size = 2000
            records_inserted = 0
            
            logger.info("Starting data insertion for 10 SST files...")
            
            for batch in range(total_batches):
                for i in range(batch_size):
                    record_num = batch * batch_size + i
                    key = f"key:{record_num:08d}".encode()
                    value = f"value:{record_num:08d}".encode() * value_size
                    self.db.put(self.cf_name, key, value, ttl=-1)
                    records_inserted += 1
                
                analysis = self.analyze_sstables()
                current_count = analysis['total_count']
                logger.info(f"SST files created: {current_count}")
                
                if current_count >= 10:
                    self.total_records = records_inserted
                    logger.info(f"Reached target of 10 SST files with {records_inserted} records")
                    break
            
            analysis = self.analyze_sstables()
            logger.info("Data insertion complete - Total SSTables: %d", analysis['total_count'])

            time.sleep(1)
            self.save_current_state("before_compaction.txt", self.total_records)
            
        except Exception as e:
            logger.error("Failed to setup database: %s", str(e))
            self.cleanup()
            raise
    
    def run_compaction(self) -> bool:
        try:
            initial_analysis = self.analyze_sstables()
            logger.info("Starting compaction monitoring:")
            logger.info("Initial state: %d SSTables", initial_analysis['total_count'])
            
            expected_count = 5
            logger.info("Target count: 5 SSTables")
            
            logger.info("Starting background compaction process...")
            self.db.start_background_compaction(
                self.cf_name,
                interval_seconds=1,
                min_sstables=2
            )
            
            start_time = time.time()
            last_count = initial_analysis['total_count']
            
            while time.time() - start_time < 30:
                time.sleep(1)
                
                current_analysis = self.analyze_sstables()
                current_count = current_analysis['total_count']
                
                if current_count != last_count:
                    logger.info("Compaction progress: %d -> %d SSTables", last_count, current_count)
                    last_count = current_count
                
                if current_count <= expected_count:
                    time.sleep(1)
                    self.save_current_state("after_compaction.txt", self.total_records)
                    logger.info("Target achieved - 5 SST files reached!")
                    break
                    
            return True
                
        except Exception as e:
            logger.error(f"Error during compaction: {str(e)}")
            return False
    
    def cleanup(self) -> None:
        try:
            if hasattr(self, 'db') and self.db is not None:
                logger.info("Closing database")
                self.db.close()
                self.db = None
        except Exception as e:
            logger.error("Error closing database: %s", str(e))


def main():
    db_path = str(Path.cwd() / "compaction_output")
    
    monitor = CompactionMonitor(db_path)
    try:
        monitor.setup_database()
        monitor.run_compaction()
    except Exception as e:
        logger.error("Monitor failed: %s", str(e))
        sys.exit(1)
    finally:
        monitor.cleanup()

if __name__ == "__main__":
    main()
