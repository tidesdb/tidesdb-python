class MockCursor:
    def __init__(self, data):
        self.data = list(data.items())
        self.position = 0

    def next(self):
        if self.position >= len(self.data) - 1:
            raise Exception("No more items")
        self.position += 1

    def prev(self):
        if self.position <= 0:
            raise Exception("No previous items")
        self.position -= 1

    def get(self):
        if 0 <= self.position < len(self.data):
            return self.data[self.position]
        raise Exception("Invalid cursor position")

    def free(self):
        self.data = None


class MockTransaction:
    def __init__(self, db, column_family):
        self.db = db
        self.column_family = column_family
        self.changes = {}
        self.deletes = set()

    def put(self, key, value, ttl):
        self.changes[key] = value

    def delete(self, key):
        self.deletes.add(key)

    def commit(self):
        if self.column_family not in self.db.data:
            raise Exception("Column family does not exist")
        
        for key, value in self.changes.items():
            self.db.data[self.column_family][key] = value
            
        for key in self.deletes:
            if key in self.db.data[self.column_family]:
                del self.db.data[self.column_family][key]

    def rollback(self):
        self.changes.clear()
        self.deletes.clear()

    def free(self):
        self.changes = None
        self.deletes = None


class MockTidesDB:
    def __init__(self):
        self.column_families = {}
        self.data = {}
        self.is_open = True

    @staticmethod
    def open(directory):
        return MockTidesDB()

    def close(self):
        self.is_open = False

    def create_column_family(self, name, flush_threshold, max_level, probability, compressed, compress_algo, bloom_filter):
        if not self.is_open:
            raise Exception("Database is closed")
        if name in self.column_families:
            raise Exception("Column family already exists")
        self.column_families[name] = {
            'flush_threshold': flush_threshold,
            'max_level': max_level,
            'probability': probability,
            'compressed': compressed,
            'compress_algo': compress_algo,
            'bloom_filter': bloom_filter
        }
        self.data[name] = {}

    def drop_column_family(self, name):
        if not self.is_open:
            raise Exception("Database is closed")
        if name not in self.column_families:
            raise Exception("Column family does not exist")
        del self.column_families[name]
        del self.data[name]

    def list_column_families(self):
        if not self.is_open:
            raise Exception("Database is closed")
        return ' '.join(self.column_families.keys())

    def compact_sstables(self, column_family_name, max_threads):
        if not self.is_open:
            raise Exception("Database is closed")
        if column_family_name not in self.column_families:
            raise Exception("Column family does not exist")

    def put(self, column_family_name, key, value, ttl):
        if not self.is_open:
            raise Exception("Database is closed")
        if column_family_name not in self.column_families:
            raise Exception("Column family does not exist")
        self.data[column_family_name][key] = value

    def get(self, column_family_name, key):
        if not self.is_open:
            raise Exception("Database is closed")
        if column_family_name not in self.column_families:
            raise Exception("Column family does not exist")
        if key not in self.data[column_family_name]:
            raise Exception("Key not found")
        return self.data[column_family_name][key]

    def delete(self, column_family_name, key):
        if not self.is_open:
            raise Exception("Database is closed")
        if column_family_name not in self.column_families:
            raise Exception("Column family does not exist")
        if key in self.data[column_family_name]:
            del self.data[column_family_name][key]

    @staticmethod
    def cursor_init(db, column_family):
        if column_family not in db.data:
            raise Exception("Column family does not exist")
        return MockCursor(db.data[column_family])

    @staticmethod
    def transaction_begin(db, column_family):
        if column_family not in db.column_families:
            raise Exception("Column family does not exist")
        return MockTransaction(db, column_family)