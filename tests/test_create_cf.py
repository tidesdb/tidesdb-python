import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tests.mock_tidesdb import MockTidesDB

class TestCreateColumnFamily(unittest.TestCase):
    def setUp(self):
        self.db = MockTidesDB.open("test.db")

    def tearDown(self):
        self.db.close()

    def test_create_cf_no_compression(self):
        self.db.create_column_family(
            name="test_family",
            flush_threshold=128 * 1024 * 1024,
            max_level=12,
            probability=0.24,
            compressed=False,
            compress_algo=NO_COMPRESSION,
            bloom_filter=False,
            memtable_ds=SKIP_LIST
        )
        families = self.db.list_column_families()
        self.assertIn("test_family", families)

    def test_create_duplicate_cf(self):
        self.db.create_column_family(
            name="test_family",
            flush_threshold=128 * 1024 * 1024,
            max_level=12,
            probability=0.24,
            compressed=False,
            compress_algo=NO_COMPRESSION,
            bloom_filter=False,
            memtable_ds=SKIP_LIST
        )
        
        with self.assertRaises(Exception):
            self.db.create_column_family(
                name="test_family",
                flush_threshold=128 * 1024 * 1024,
                max_level=12,
                probability=0.24,
                compressed=False,
                compress_algo=NO_COMPRESSION,
                bloom_filter=False,
                memtable_ds=SKIP_LIST
            )

if __name__ == '__main__':
    unittest.main()