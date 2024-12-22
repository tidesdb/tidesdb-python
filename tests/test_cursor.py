import unittest
from tests.mock_tidesdb import MockTidesDB

class TestCursor(unittest.TestCase):
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
        

        self.db.put("test_family", b"key1", b"value1", 0)
        self.db.put("test_family", b"key2", b"value2", 0)
        self.db.put("test_family", b"key3", b"value3", 0)

    def tearDown(self):
        self.db.close()

    def test_cursor_next(self):
        cursor = self.db.cursor_init(self.db, "test_family")
        key, value = cursor.get()
        self.assertEqual(value, b"value1")
        cursor.next()
        key, value = cursor.get()
        self.assertEqual(value, b"value2")
        cursor.free()

    def test_cursor_prev(self):
        cursor = self.db.cursor_init(self.db, "test_family")
        cursor.next() 
        key, value = cursor.get()
        cursor.prev()
        key, value = cursor.get()
        self.assertEqual(value, b"value1")
        cursor.free()

if __name__ == '__main__':
    unittest.main()