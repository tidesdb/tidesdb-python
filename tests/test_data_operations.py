import unittest
from tests.mock_tidesdb import MockTidesDB

class TestDataOperations(unittest.TestCase):
    def setUp(self):
        self.db = MockTidesDB.open("test.db")
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

    def tearDown(self):
        self.db.close()

    def test_put_get(self):
        key = b"test_key"
        value = b"test_value"
        ttl = 0
        
        self.db.put("test_family", key, value, ttl)
        result = self.db.get("test_family", key)
        self.assertEqual(result, value)

    def test_delete(self):
        key = b"delete_key"
        value = b"delete_value"
        ttl = 0
        
        self.db.put("test_family", key, value, ttl)
        self.db.delete("test_family", key)
        
        with self.assertRaises(Exception):
            self.db.get("test_family", key)

    def test_nonexistent_key(self):
        with self.assertRaises(Exception):
            self.db.get("test_family", b"nonexistent")

if __name__ == '__main__':
    unittest.main()