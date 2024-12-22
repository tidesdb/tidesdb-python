import unittest
from tests.mock_tidesdb import MockTidesDB

class TestTidesDBBase(unittest.TestCase):
    def test_open_close(self):
        db = MockTidesDB.open("test.db")
        self.assertTrue(db.is_open)
        db.close()
        self.assertFalse(db.is_open)

    def test_operations_after_close(self):
        db = MockTidesDB.open("test.db")
        db.close()
        with self.assertRaises(Exception):
            db.create_column_family(
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