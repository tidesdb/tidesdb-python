import unittest
from tests.mock_tidesdb import MockTidesDB

class TestTransaction(unittest.TestCase):
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

    def test_transaction_commit(self):
        txn = self.db.transaction_begin(self.db, "test_family")
        key = b"txn_key"
        value = b"txn_value"
        txn.put(key, value, ttl=0)
        txn.commit()
        txn.free()
        
        result = self.db.get("test_family", key)
        self.assertEqual(result, value)

    def test_transaction_rollback(self):
        txn = self.db.transaction_begin(self.db, "test_family")
        key = b"txn_key"
        value = b"txn_value"
        txn.put(key, value, ttl=0)
        txn.rollback()
        txn.free()
        
        with self.assertRaises(Exception):
            self.db.get("test_family", key)

if __name__ == '__main__':
    unittest.main()