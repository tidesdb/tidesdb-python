"""
Tests for TidesDB Python bindings.

These tests require the TidesDB shared library to be installed.
"""

import os
import shutil
import tempfile
import time

import pytest

import tidesdb


@pytest.fixture
def temp_db_path():
    """Create a temporary directory for test database."""
    path = tempfile.mkdtemp(prefix="tidesdb_test_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def db(temp_db_path):
    """Create a test database."""
    database = tidesdb.TidesDB.open(temp_db_path)
    yield database
    database.close()


@pytest.fixture
def cf(db):
    """Create a test column family."""
    db.create_column_family("test_cf")
    cf = db.get_column_family("test_cf")
    yield cf
    try:
        db.drop_column_family("test_cf")
    except tidesdb.TidesDBError:
        pass


class TestOpenClose:
    """Tests for database open/close operations."""

    def test_open_close(self, temp_db_path):
        """Test basic open and close."""
        db = tidesdb.TidesDB.open(temp_db_path)
        assert db is not None
        db.close()

    def test_open_with_config(self, temp_db_path):
        """Test open with custom configuration."""
        config = tidesdb.Config(
            db_path=temp_db_path,
            num_flush_threads=4,
            num_compaction_threads=4,
            log_level=tidesdb.LogLevel.LOG_WARN,
            block_cache_size=32 * 1024 * 1024,
            max_open_sstables=128,
        )
        db = tidesdb.TidesDB(config)
        assert db is not None
        db.close()

    def test_context_manager(self, temp_db_path):
        """Test database as context manager."""
        with tidesdb.TidesDB.open(temp_db_path) as db:
            assert db is not None


class TestColumnFamilies:
    """Tests for column family operations."""

    def test_create_drop_column_family(self, db):
        """Test creating and dropping a column family."""
        db.create_column_family("test_cf")
        cf = db.get_column_family("test_cf")
        assert cf is not None
        assert cf.name == "test_cf"
        db.drop_column_family("test_cf")

    def test_create_with_config(self, db):
        """Test creating column family with custom config."""
        config = tidesdb.default_column_family_config()
        config.write_buffer_size = 32 * 1024 * 1024
        config.compression_algorithm = tidesdb.CompressionAlgorithm.LZ4_COMPRESSION
        config.enable_bloom_filter = True
        config.bloom_fpr = 0.01

        db.create_column_family("custom_cf", config)
        cf = db.get_column_family("custom_cf")
        assert cf is not None

        stats = cf.get_stats()
        assert stats.config is not None
        assert stats.config.enable_bloom_filter is True

        db.drop_column_family("custom_cf")

    def test_list_column_families(self, db):
        """Test listing column families."""
        db.create_column_family("cf1")
        db.create_column_family("cf2")

        names = db.list_column_families()
        assert "cf1" in names
        assert "cf2" in names

        db.drop_column_family("cf1")
        db.drop_column_family("cf2")

    def test_get_nonexistent_column_family(self, db):
        """Test getting a non-existent column family."""
        with pytest.raises(tidesdb.TidesDBError):
            db.get_column_family("nonexistent")


class TestTransactions:
    """Tests for transaction operations."""

    def test_put_get(self, db, cf):
        """Test basic put and get."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        with db.begin_txn() as txn:
            value = txn.get(cf, b"key1")
            assert value == b"value1"

    def test_delete(self, db, cf):
        """Test delete operation."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        with db.begin_txn() as txn:
            txn.delete(cf, b"key1")
            txn.commit()

        with db.begin_txn() as txn:
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"key1")

    def test_rollback(self, db, cf):
        """Test transaction rollback."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.rollback()

        with db.begin_txn() as txn:
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"key1")

    def test_multiple_operations(self, db, cf):
        """Test multiple operations in one transaction."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.put(cf, b"key2", b"value2")
            txn.put(cf, b"key3", b"value3")
            txn.delete(cf, b"key2")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key1") == b"value1"
            assert txn.get(cf, b"key3") == b"value3"
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"key2")

    def test_isolation_level(self, db, cf):
        """Test transaction with specific isolation level."""
        txn = db.begin_txn_with_isolation(tidesdb.IsolationLevel.SERIALIZABLE)
        txn.put(cf, b"key1", b"value1")
        txn.commit()
        txn.close()


class TestSavepoints:
    """Tests for savepoint operations."""

    def test_savepoint_rollback(self, db, cf):
        """Test savepoint and rollback to savepoint."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.savepoint("sp1")
            txn.put(cf, b"key2", b"value2")
            txn.rollback_to_savepoint("sp1")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key1") == b"value1"
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"key2")

    def test_release_savepoint(self, db, cf):
        """Test releasing a savepoint."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.savepoint("sp1")
            txn.put(cf, b"key2", b"value2")
            txn.release_savepoint("sp1")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key1") == b"value1"
            assert txn.get(cf, b"key2") == b"value2"


class TestIterators:
    """Tests for iterator operations."""

    def test_forward_iteration(self, db, cf):
        """Test forward iteration."""
        with db.begin_txn() as txn:
            txn.put(cf, b"a", b"1")
            txn.put(cf, b"b", b"2")
            txn.put(cf, b"c", b"3")
            txn.commit()

        with db.begin_txn() as txn:
            with txn.new_iterator(cf) as it:
                it.seek_to_first()
                items = list(it)
                assert len(items) == 3
                assert items[0] == (b"a", b"1")
                assert items[1] == (b"b", b"2")
                assert items[2] == (b"c", b"3")

    def test_backward_iteration(self, db, cf):
        """Test backward iteration."""
        with db.begin_txn() as txn:
            txn.put(cf, b"a", b"1")
            txn.put(cf, b"b", b"2")
            txn.put(cf, b"c", b"3")
            txn.commit()

        with db.begin_txn() as txn:
            with txn.new_iterator(cf) as it:
                it.seek_to_last()
                items = []
                while it.valid():
                    items.append((it.key(), it.value()))
                    it.prev()
                assert len(items) == 3
                assert items[0] == (b"c", b"3")
                assert items[1] == (b"b", b"2")
                assert items[2] == (b"a", b"1")

    def test_seek(self, db, cf):
        """Test seek operations."""
        with db.begin_txn() as txn:
            txn.put(cf, b"a", b"1")
            txn.put(cf, b"c", b"3")
            txn.put(cf, b"e", b"5")
            txn.commit()

        with db.begin_txn() as txn:
            with txn.new_iterator(cf) as it:
                it.seek(b"b")
                assert it.valid()
                assert it.key() == b"c"

                it.seek_for_prev(b"d")
                assert it.valid()
                assert it.key() == b"c"


class TestTTL:
    """Tests for TTL functionality."""

    def test_ttl_expiration(self, db, cf):
        """Test that keys with expired TTL are not returned."""
        expired_ttl = int(time.time()) - 1

        with db.begin_txn() as txn:
            txn.put(cf, b"expired_key", b"value", ttl=expired_ttl)
            txn.commit()

        with db.begin_txn() as txn:
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"expired_key")

    def test_no_ttl(self, db, cf):
        """Test that keys without TTL persist."""
        with db.begin_txn() as txn:
            txn.put(cf, b"permanent_key", b"value", ttl=-1)
            txn.commit()

        with db.begin_txn() as txn:
            value = txn.get(cf, b"permanent_key")
            assert value == b"value"


class TestStats:
    """Tests for statistics operations."""

    def test_column_family_stats(self, db, cf):
        """Test getting column family statistics."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        stats = cf.get_stats()
        assert stats.num_levels >= 0
        assert stats.memtable_size >= 0

    def test_cache_stats(self, db):
        """Test getting cache statistics."""
        stats = db.get_cache_stats()
        assert isinstance(stats.enabled, bool)
        assert stats.hits >= 0
        assert stats.misses >= 0


class TestMaintenance:
    """Tests for maintenance operations."""

    def test_flush_memtable(self, db, cf):
        """Test manual memtable flush."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        cf.flush_memtable()
        time.sleep(0.5)

    def test_compact(self, db, cf):
        """Test manual compaction."""
        with db.begin_txn() as txn:
            for i in range(100):
                txn.put(cf, f"key{i}".encode(), f"value{i}".encode())
            txn.commit()

        cf.flush_memtable()
        time.sleep(0.5)
        try:
            cf.compact()
        except tidesdb.TidesDBError:
            pass
        time.sleep(0.5)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
