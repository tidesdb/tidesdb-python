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

    def test_create_with_btree_config(self, db):
        """Test creating column family with B+tree format enabled."""
        config = tidesdb.default_column_family_config()
        config.use_btree = True

        db.create_column_family("btree_cf", config)
        cf = db.get_column_family("btree_cf")
        assert cf is not None

        stats = cf.get_stats()
        assert stats.config is not None
        assert stats.config.use_btree is True
        assert stats.use_btree is True

        db.drop_column_family("btree_cf")

    def test_default_config_use_btree(self, db):
        """Test that default config has use_btree=False."""
        config = tidesdb.default_column_family_config()
        assert config.use_btree is False

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
        """Test that keys with expired TTL are eventually not returned."""
        expired_ttl = int(time.time()) - 1

        with db.begin_txn() as txn:
            txn.put(cf, b"expired_key", b"value", ttl=expired_ttl)
            txn.commit()

        cf.flush_memtable()
        time.sleep(2)

        with db.begin_txn() as txn:
            try:
                txn.get(cf, b"expired_key")
            except tidesdb.TidesDBError:
                pass

    def test_no_ttl(self, db, cf):
        """Test that keys without TTL persist."""
        with db.begin_txn() as txn:
            txn.put(cf, b"permanent_key", b"value", ttl=-1)
            txn.commit()

        with db.begin_txn() as txn:
            value = txn.get(cf, b"permanent_key")
            assert value == b"value"


class TestCloneColumnFamily:
    """Tests for column family clone operations."""

    def test_clone_column_family(self, db, cf):
        """Test cloning a column family with data."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.put(cf, b"key2", b"value2")
            txn.commit()

        db.clone_column_family("test_cf", "cloned_cf")

        cloned = db.get_column_family("cloned_cf")
        assert cloned is not None
        assert cloned.name == "cloned_cf"

        with db.begin_txn() as txn:
            assert txn.get(cloned, b"key1") == b"value1"
            assert txn.get(cloned, b"key2") == b"value2"

        db.drop_column_family("cloned_cf")

    def test_clone_independence(self, db, cf):
        """Test that clone is independent from source."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"original")
            txn.commit()

        db.clone_column_family("test_cf", "cloned_cf")
        cloned = db.get_column_family("cloned_cf")

        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"modified")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cloned, b"key1") == b"original"
            assert txn.get(cf, b"key1") == b"modified"

        db.drop_column_family("cloned_cf")

    def test_clone_nonexistent_source(self, db):
        """Test cloning a non-existent column family raises error."""
        with pytest.raises(tidesdb.TidesDBError):
            db.clone_column_family("nonexistent", "cloned_cf")

    def test_clone_to_existing_name(self, db, cf):
        """Test cloning to an already existing name raises error."""
        db.create_column_family("existing_cf")
        with pytest.raises(tidesdb.TidesDBError):
            db.clone_column_family("test_cf", "existing_cf")
        db.drop_column_family("existing_cf")

    def test_clone_listed(self, db, cf):
        """Test that cloned column family appears in list."""
        db.clone_column_family("test_cf", "cloned_cf")

        names = db.list_column_families()
        assert "test_cf" in names
        assert "cloned_cf" in names

        db.drop_column_family("cloned_cf")


class TestTransactionReset:
    """Tests for transaction reset operations."""

    def test_reset_after_commit(self, db, cf):
        """Test resetting a transaction after commit."""
        txn = db.begin_txn()
        txn.put(cf, b"key1", b"value1")
        txn.commit()

        txn.reset(tidesdb.IsolationLevel.READ_COMMITTED)

        txn.put(cf, b"key2", b"value2")
        txn.commit()
        txn.close()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key1") == b"value1"
            assert txn.get(cf, b"key2") == b"value2"

    def test_reset_after_rollback(self, db, cf):
        """Test resetting a transaction after rollback."""
        txn = db.begin_txn()
        txn.put(cf, b"key1", b"value1")
        txn.rollback()

        txn.reset(tidesdb.IsolationLevel.READ_COMMITTED)

        txn.put(cf, b"key2", b"value2")
        txn.commit()
        txn.close()

        with db.begin_txn() as txn:
            with pytest.raises(tidesdb.TidesDBError):
                txn.get(cf, b"key1")
            assert txn.get(cf, b"key2") == b"value2"

    def test_reset_with_different_isolation(self, db, cf):
        """Test resetting with a different isolation level."""
        txn = db.begin_txn()
        txn.put(cf, b"key1", b"value1")
        txn.commit()

        txn.reset(tidesdb.IsolationLevel.SERIALIZABLE)

        txn.put(cf, b"key2", b"value2")
        txn.commit()
        txn.close()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key1") == b"value1"
            assert txn.get(cf, b"key2") == b"value2"

    def test_reset_reuse_loop(self, db, cf):
        """Test resetting in a loop for batch processing."""
        txn = db.begin_txn()

        for i in range(5):
            txn.put(cf, f"batch_key_{i}".encode(), f"batch_value_{i}".encode())
            txn.commit()
            if i < 4:
                txn.reset(tidesdb.IsolationLevel.READ_COMMITTED)

        txn.close()

        with db.begin_txn() as txn:
            for i in range(5):
                value = txn.get(cf, f"batch_key_{i}".encode())
                assert value == f"batch_value_{i}".encode()

    def test_reset_closed_transaction_raises(self, db, cf):
        """Test that resetting a closed transaction raises error."""
        txn = db.begin_txn()
        txn.put(cf, b"key1", b"value1")
        txn.commit()
        txn.close()

        with pytest.raises(tidesdb.TidesDBError):
            txn.reset(tidesdb.IsolationLevel.READ_COMMITTED)


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

    def test_column_family_stats_btree_fields(self, db, cf):
        """Test that B+tree stats fields are present."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        stats = cf.get_stats()
        # B+tree stats should be present (even if 0 for non-btree CF)
        assert isinstance(stats.use_btree, bool)
        assert isinstance(stats.btree_total_nodes, int)
        assert isinstance(stats.btree_max_height, int)
        assert isinstance(stats.btree_avg_height, float)
        assert stats.btree_total_nodes >= 0
        assert stats.btree_max_height >= 0
        assert stats.btree_avg_height >= 0.0

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


class TestCheckpoint:
    """Tests for checkpoint operations."""

    def test_checkpoint_creates_snapshot(self, db, cf, temp_db_path):
        """Test that checkpoint creates a usable snapshot."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.put(cf, b"key2", b"value2")
            txn.commit()

        checkpoint_dir = temp_db_path + "_checkpoint"
        try:
            db.checkpoint(checkpoint_dir)
            assert os.path.isdir(checkpoint_dir)

            with tidesdb.TidesDB.open(checkpoint_dir) as checkpoint_db:
                cp_cf = checkpoint_db.get_column_family("test_cf")
                with checkpoint_db.begin_txn() as txn:
                    assert txn.get(cp_cf, b"key1") == b"value1"
                    assert txn.get(cp_cf, b"key2") == b"value2"
        finally:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)

    def test_checkpoint_existing_dir_raises(self, db, cf, temp_db_path):
        """Test that checkpoint to a non-empty directory raises error."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        checkpoint_dir = temp_db_path + "_checkpoint"
        try:
            db.checkpoint(checkpoint_dir)

            with pytest.raises(tidesdb.TidesDBError):
                db.checkpoint(checkpoint_dir)
        finally:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)

    def test_checkpoint_independence(self, db, cf, temp_db_path):
        """Test that checkpoint is independent from the live database."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"original")
            txn.commit()

        checkpoint_dir = temp_db_path + "_checkpoint"
        try:
            db.checkpoint(checkpoint_dir)

            with db.begin_txn() as txn:
                txn.put(cf, b"key1", b"modified")
                txn.commit()

            with tidesdb.TidesDB.open(checkpoint_dir) as checkpoint_db:
                cp_cf = checkpoint_db.get_column_family("test_cf")
                with checkpoint_db.begin_txn() as txn:
                    assert txn.get(cp_cf, b"key1") == b"original"
        finally:
            shutil.rmtree(checkpoint_dir, ignore_errors=True)


class TestRangeCost:
    """Tests for range cost estimation."""

    def test_range_cost_returns_float(self, db, cf):
        """Test that range_cost returns a float value."""
        with db.begin_txn() as txn:
            txn.put(cf, b"key_a", b"value_a")
            txn.put(cf, b"key_z", b"value_z")
            txn.commit()

        cost = cf.range_cost(b"key_a", b"key_z")
        assert isinstance(cost, float)
        assert cost >= 0.0

    def test_range_cost_empty_cf(self, db, cf):
        """Test range_cost on an empty column family."""
        cost = cf.range_cost(b"a", b"z")
        assert isinstance(cost, float)
        assert cost >= 0.0

    def test_range_cost_key_order_irrelevant(self, db, cf):
        """Test that key order does not matter."""
        with db.begin_txn() as txn:
            txn.put(cf, b"aaa", b"1")
            txn.put(cf, b"zzz", b"2")
            txn.commit()

        cost_ab = cf.range_cost(b"aaa", b"zzz")
        cost_ba = cf.range_cost(b"zzz", b"aaa")
        assert cost_ab == cost_ba

    def test_range_cost_narrow_vs_wide(self, db, cf):
        """Test that a wider range costs at least as much as a narrow one."""
        with db.begin_txn() as txn:
            for i in range(50):
                txn.put(cf, f"key:{i:04d}".encode(), f"val:{i}".encode())
            txn.commit()

        narrow = cf.range_cost(b"key:0010", b"key:0015")
        wide = cf.range_cost(b"key:0000", b"key:0049")
        # Wide range should generally cost >= narrow range
        assert wide >= narrow

    def test_range_cost_comparison(self, db, cf):
        """Test comparing costs of different ranges."""
        with db.begin_txn() as txn:
            for i in range(100):
                txn.put(cf, f"user:{i:04d}".encode(), f"data:{i}".encode())
            txn.commit()

        cost_a = cf.range_cost(b"user:0000", b"user:0009")
        cost_b = cf.range_cost(b"user:0000", b"user:0099")
        # Both should be valid floats
        assert isinstance(cost_a, float)
        assert isinstance(cost_b, float)


class TestLoadConfigFromIni:
    """Tests for loading column family config from INI files."""

    def test_save_and_load_roundtrip(self, temp_db_path):
        """Test that saving and loading config produces equivalent results."""
        original = tidesdb.default_column_family_config()
        original.write_buffer_size = 32 * 1024 * 1024
        original.compression_algorithm = tidesdb.CompressionAlgorithm.ZSTD_COMPRESSION
        original.enable_bloom_filter = True
        original.bloom_fpr = 0.001
        original.sync_mode = tidesdb.SyncMode.SYNC_FULL
        original.min_levels = 7
        original.use_btree = True

        ini_path = os.path.join(temp_db_path, "test_config.ini")
        tidesdb.save_config_to_ini(ini_path, "my_cf", original)

        loaded = tidesdb.load_config_from_ini(ini_path, "my_cf")

        assert loaded.write_buffer_size == original.write_buffer_size
        assert loaded.compression_algorithm == original.compression_algorithm
        assert loaded.enable_bloom_filter == original.enable_bloom_filter
        assert abs(loaded.bloom_fpr - original.bloom_fpr) < 1e-9
        assert loaded.sync_mode == original.sync_mode
        assert loaded.min_levels == original.min_levels
        assert loaded.use_btree == original.use_btree

    def test_load_nonexistent_file_raises(self, temp_db_path):
        """Test that loading from a non-existent file raises error."""
        ini_path = os.path.join(temp_db_path, "nonexistent.ini")
        with pytest.raises(tidesdb.TidesDBError):
            tidesdb.load_config_from_ini(ini_path, "my_cf")

    def test_load_preserves_all_fields(self, temp_db_path):
        """Test that all configuration fields survive a save/load roundtrip."""
        original = tidesdb.default_column_family_config()
        original.level_size_ratio = 8
        original.dividing_level_offset = 3
        original.klog_value_threshold = 1024
        original.index_sample_ratio = 2
        original.block_index_prefix_len = 32
        original.sync_interval_us = 500000
        original.skip_list_max_level = 16
        original.skip_list_probability = 0.5
        original.min_disk_space = 200 * 1024 * 1024
        original.l1_file_count_trigger = 8
        original.l0_queue_stall_threshold = 30

        ini_path = os.path.join(temp_db_path, "full_config.ini")
        tidesdb.save_config_to_ini(ini_path, "full_cf", original)

        loaded = tidesdb.load_config_from_ini(ini_path, "full_cf")

        assert loaded.level_size_ratio == original.level_size_ratio
        assert loaded.dividing_level_offset == original.dividing_level_offset
        assert loaded.klog_value_threshold == original.klog_value_threshold
        assert loaded.index_sample_ratio == original.index_sample_ratio
        assert loaded.block_index_prefix_len == original.block_index_prefix_len
        assert loaded.sync_interval_us == original.sync_interval_us
        assert loaded.skip_list_max_level == original.skip_list_max_level
        assert loaded.min_disk_space == original.min_disk_space
        assert loaded.l1_file_count_trigger == original.l1_file_count_trigger
        assert loaded.l0_queue_stall_threshold == original.l0_queue_stall_threshold


class TestCommitHook:
    """Tests for commit hook (change data capture) API."""

    def test_hook_fires_on_commit(self, db, cf):
        """Test that the commit hook fires when a transaction commits."""
        captured = []

        def hook(ops, commit_seq):
            captured.append({"ops": ops, "seq": commit_seq})
            return 0

        cf.set_commit_hook(hook)

        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        assert len(captured) == 1
        assert len(captured[0]["ops"]) == 1
        assert captured[0]["ops"][0].key == b"key1"
        assert captured[0]["ops"][0].value == b"value1"
        assert captured[0]["ops"][0].is_delete is False
        assert captured[0]["seq"] > 0

        cf.clear_commit_hook()

    def test_hook_captures_deletes(self, db, cf):
        """Test that the hook correctly reports delete operations."""
        captured = []

        with db.begin_txn() as txn:
            txn.put(cf, b"del_key", b"del_val")
            txn.commit()

        def hook(ops, commit_seq):
            captured.append(ops)
            return 0

        cf.set_commit_hook(hook)

        with db.begin_txn() as txn:
            txn.delete(cf, b"del_key")
            txn.commit()

        assert len(captured) == 1
        delete_ops = [op for op in captured[0] if op.is_delete]
        assert len(delete_ops) >= 1
        assert delete_ops[0].key == b"del_key"
        assert delete_ops[0].value is None

        cf.clear_commit_hook()

    def test_hook_multi_op_batch(self, db, cf):
        """Test that the hook receives all operations in a batch."""
        captured = []

        def hook(ops, commit_seq):
            captured.append(ops)
            return 0

        cf.set_commit_hook(hook)

        with db.begin_txn() as txn:
            txn.put(cf, b"batch1", b"v1")
            txn.put(cf, b"batch2", b"v2")
            txn.put(cf, b"batch3", b"v3")
            txn.commit()

        assert len(captured) == 1
        assert len(captured[0]) == 3
        keys = {op.key for op in captured[0]}
        assert keys == {b"batch1", b"batch2", b"batch3"}

        cf.clear_commit_hook()

    def test_hook_commit_seq_increases(self, db, cf):
        """Test that commit_seq is monotonically increasing."""
        seqs = []

        def hook(ops, commit_seq):
            seqs.append(commit_seq)
            return 0

        cf.set_commit_hook(hook)

        for i in range(3):
            with db.begin_txn() as txn:
                txn.put(cf, f"seq_key_{i}".encode(), f"seq_val_{i}".encode())
                txn.commit()

        assert len(seqs) == 3
        assert seqs[0] < seqs[1] < seqs[2]

        cf.clear_commit_hook()

    def test_clear_hook_stops_firing(self, db, cf):
        """Test that clearing the hook stops callbacks."""
        captured = []

        def hook(ops, commit_seq):
            captured.append(ops)
            return 0

        cf.set_commit_hook(hook)

        with db.begin_txn() as txn:
            txn.put(cf, b"before_clear", b"v")
            txn.commit()

        assert len(captured) == 1

        cf.clear_commit_hook()

        with db.begin_txn() as txn:
            txn.put(cf, b"after_clear", b"v")
            txn.commit()

        # No new captures after clearing
        assert len(captured) == 1

    def test_hook_failure_does_not_rollback(self, db, cf):
        """Test that a hook returning non-zero does not affect the commit."""
        def failing_hook(ops, commit_seq):
            return -1  # Simulate failure

        cf.set_commit_hook(failing_hook)

        with db.begin_txn() as txn:
            txn.put(cf, b"survive", b"value")
            txn.commit()

        cf.clear_commit_hook()

        # Data should still be committed despite hook failure
        with db.begin_txn() as txn:
            assert txn.get(cf, b"survive") == b"value"

    def test_hook_with_ttl(self, db, cf):
        """Test that the hook reports TTL values."""
        captured = []

        def hook(ops, commit_seq):
            captured.append(ops)
            return 0

        cf.set_commit_hook(hook)

        ttl_val = int(time.time()) + 3600  # 1 hour from now
        with db.begin_txn() as txn:
            txn.put(cf, b"ttl_key", b"ttl_val", ttl=ttl_val)
            txn.commit()

        assert len(captured) == 1
        assert captured[0][0].ttl == ttl_val

        cf.clear_commit_hook()

    def test_hook_exception_handled(self, db, cf):
        """Test that Python exceptions in the hook don't crash the process."""
        def crashing_hook(ops, commit_seq):
            raise RuntimeError("hook crashed!")

        cf.set_commit_hook(crashing_hook)

        # Should not raise - exception is caught internally
        with db.begin_txn() as txn:
            txn.put(cf, b"crash_key", b"crash_val")
            txn.commit()

        cf.clear_commit_hook()

        # Data should still be committed
        with db.begin_txn() as txn:
            assert txn.get(cf, b"crash_key") == b"crash_val"


class TestGetComparator:
    """Tests for get_comparator operations."""

    def test_builtin_comparator_exists(self, db):
        """Test that built-in comparators are registered."""
        assert db.get_comparator("memcmp") is True
        assert db.get_comparator("reverse") is True
        assert db.get_comparator("lexicographic") is True

    def test_nonexistent_comparator(self, db):
        """Test that a non-existent comparator returns False."""
        assert db.get_comparator("nonexistent_comp") is False

    def test_custom_comparator_registered(self, db):
        """Test that a custom registered comparator is found."""
        def my_cmp(k1: bytes, k2: bytes) -> int:
            if k1 < k2:
                return -1
            elif k1 > k2:
                return 1
            return 0

        db.register_comparator("my_test_cmp", my_cmp)
        assert db.get_comparator("my_test_cmp") is True


class TestDeleteColumnFamily:
    """Tests for delete_column_family (by pointer) operations."""

    def test_delete_by_pointer(self, db):
        """Test deleting a column family by pointer."""
        db.create_column_family("del_cf")
        cf = db.get_column_family("del_cf")
        assert cf is not None

        db.delete_column_family(cf)

        with pytest.raises(tidesdb.TidesDBError):
            db.get_column_family("del_cf")

    def test_delete_with_data(self, db):
        """Test deleting a column family that contains data."""
        db.create_column_family("data_cf")
        cf = db.get_column_family("data_cf")

        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        db.delete_column_family(cf)

        with pytest.raises(tidesdb.TidesDBError):
            db.get_column_family("data_cf")


class TestMaxMemoryUsage:
    """Tests for max_memory_usage configuration."""

    def test_config_default(self):
        """Test that default max_memory_usage is 0."""
        config = tidesdb.Config(db_path="/tmp/test")
        assert config.max_memory_usage == 0

    def test_open_with_max_memory_usage(self, temp_db_path):
        """Test opening a database with max_memory_usage set."""
        db = tidesdb.TidesDB.open(temp_db_path, max_memory_usage=0)
        assert db is not None
        db.close()

    def test_config_with_max_memory_usage(self, temp_db_path):
        """Test Config with explicit max_memory_usage."""
        config = tidesdb.Config(
            db_path=temp_db_path,
            max_memory_usage=512 * 1024 * 1024,  # 512MB
        )
        db = tidesdb.TidesDB(config)
        assert db is not None
        db.close()


class TestPurgeCf:
    """Tests for column family purge operations."""

    def test_purge_cf_basic(self, db, cf):
        """Test purging a column family with data."""
        with db.begin_txn() as txn:
            for i in range(50):
                txn.put(cf, f"purge_key_{i}".encode(), f"purge_val_{i}".encode())
            txn.commit()

        cf.purge()

        # Data should still be accessible after purge
        with db.begin_txn() as txn:
            for i in range(50):
                value = txn.get(cf, f"purge_key_{i}".encode())
                assert value == f"purge_val_{i}".encode()

    def test_purge_cf_empty(self, db, cf):
        """Test purging an empty column family succeeds."""
        cf.purge()

    def test_purge_cf_after_deletes(self, db, cf):
        """Test purge after bulk deletes reclaims tombstones."""
        with db.begin_txn() as txn:
            for i in range(20):
                txn.put(cf, f"del_key_{i}".encode(), f"del_val_{i}".encode())
            txn.commit()

        with db.begin_txn() as txn:
            for i in range(20):
                txn.delete(cf, f"del_key_{i}".encode())
            txn.commit()

        cf.purge()

        # Deleted keys should not be found
        with db.begin_txn() as txn:
            for i in range(20):
                with pytest.raises(tidesdb.TidesDBError):
                    txn.get(cf, f"del_key_{i}".encode())


class TestPurgeDb:
    """Tests for database-level purge operations."""

    def test_purge_db_basic(self, db):
        """Test purging the entire database."""
        db.create_column_family("purge_cf1")
        db.create_column_family("purge_cf2")
        cf1 = db.get_column_family("purge_cf1")
        cf2 = db.get_column_family("purge_cf2")

        with db.begin_txn() as txn:
            txn.put(cf1, b"k1", b"v1")
            txn.put(cf2, b"k2", b"v2")
            txn.commit()

        db.purge()

        # Data should still be accessible
        with db.begin_txn() as txn:
            assert txn.get(cf1, b"k1") == b"v1"
            assert txn.get(cf2, b"k2") == b"v2"

        db.drop_column_family("purge_cf1")
        db.drop_column_family("purge_cf2")

    def test_purge_db_empty(self, db):
        """Test purging a database with no column families succeeds."""
        db.purge()


class TestSyncWal:
    """Tests for WAL sync operations."""

    def test_sync_wal_basic(self, db, cf):
        """Test syncing WAL after writes."""
        with db.begin_txn() as txn:
            txn.put(cf, b"sync_key", b"sync_val")
            txn.commit()

        cf.sync_wal()

        # Data should be accessible after sync
        with db.begin_txn() as txn:
            assert txn.get(cf, b"sync_key") == b"sync_val"

    def test_sync_wal_empty_cf(self, db, cf):
        """Test syncing WAL on an empty column family."""
        cf.sync_wal()

    def test_sync_wal_after_batch(self, db, cf):
        """Test syncing WAL after a batch of writes."""
        with db.begin_txn() as txn:
            for i in range(100):
                txn.put(cf, f"batch_{i}".encode(), f"val_{i}".encode())
            txn.commit()

        cf.sync_wal()

        with db.begin_txn() as txn:
            for i in range(100):
                value = txn.get(cf, f"batch_{i}".encode())
                assert value == f"val_{i}".encode()


class TestDbStats:
    """Tests for database-level statistics."""

    def test_db_stats_basic(self, db):
        """Test getting database stats."""
        stats = db.get_db_stats()
        assert isinstance(stats, tidesdb.DbStats)
        assert stats.num_column_families >= 0
        assert stats.total_memory > 0
        assert stats.resolved_memory_limit > 0
        assert stats.memory_pressure_level >= 0
        assert stats.global_seq >= 0

    def test_db_stats_with_cf(self, db, cf):
        """Test that db stats reflect column family count."""
        stats = db.get_db_stats()
        assert stats.num_column_families >= 1

    def test_db_stats_after_writes(self, db, cf):
        """Test db stats after writing data."""
        with db.begin_txn() as txn:
            for i in range(50):
                txn.put(cf, f"stats_key_{i}".encode(), f"stats_val_{i}".encode())
            txn.commit()

        stats = db.get_db_stats()
        assert stats.num_column_families >= 1
        assert stats.total_memtable_bytes >= 0
        assert stats.total_sstable_count >= 0
        assert stats.total_data_size_bytes >= 0

    def test_db_stats_all_fields_present(self, db):
        """Test that all DbStats fields are populated."""
        stats = db.get_db_stats()
        assert isinstance(stats.num_column_families, int)
        assert isinstance(stats.total_memory, int)
        assert isinstance(stats.available_memory, int)
        assert isinstance(stats.resolved_memory_limit, int)
        assert isinstance(stats.memory_pressure_level, int)
        assert isinstance(stats.flush_pending_count, int)
        assert isinstance(stats.total_memtable_bytes, int)
        assert isinstance(stats.total_immutable_count, int)
        assert isinstance(stats.total_sstable_count, int)
        assert isinstance(stats.total_data_size_bytes, int)
        assert isinstance(stats.num_open_sstables, int)
        assert isinstance(stats.global_seq, int)
        assert isinstance(stats.txn_memory_bytes, int)
        assert isinstance(stats.compaction_queue_size, int)
        assert isinstance(stats.flush_queue_size, int)

    def test_db_stats_after_purge(self, db, cf):
        """Test db stats after purging."""
        with db.begin_txn() as txn:
            for i in range(20):
                txn.put(cf, f"purge_stats_{i}".encode(), f"val_{i}".encode())
            txn.commit()

        db.purge()

        stats = db.get_db_stats()
        # After purge, flush and compaction queues should be drained
        assert stats.flush_queue_size == 0
        assert stats.compaction_queue_size == 0


class TestUnifiedMemtableConfig:
    """Tests for unified memtable configuration fields."""

    def test_config_defaults(self):
        """Test unified memtable config defaults."""
        config = tidesdb.Config(db_path="/tmp/test")
        assert config.unified_memtable is False
        assert config.unified_memtable_write_buffer_size == 0
        assert config.unified_memtable_skip_list_max_level == 0
        assert config.unified_memtable_skip_list_probability == 0.0
        assert config.unified_memtable_sync_mode == tidesdb.SyncMode.SYNC_NONE
        assert config.unified_memtable_sync_interval_us == 0

    def test_open_with_unified_memtable(self, temp_db_path):
        """Test opening database with unified memtable enabled."""
        db = tidesdb.TidesDB.open(
            temp_db_path,
            unified_memtable=True,
            unified_memtable_write_buffer_size=32 * 1024 * 1024,
            unified_memtable_skip_list_max_level=12,
            unified_memtable_skip_list_probability=0.25,
        )
        db.create_column_family("test_cf")
        cf = db.get_column_family("test_cf")

        with db.begin_txn() as txn:
            txn.put(cf, b"key1", b"value1")
            txn.commit()

        with db.begin_txn() as txn:
            val = txn.get(cf, b"key1")
            assert val == b"value1"

        db.close()

    def test_open_with_config_object(self, temp_db_path):
        """Test opening database with Config object containing unified memtable fields."""
        config = tidesdb.Config(
            db_path=temp_db_path,
            unified_memtable=True,
            unified_memtable_write_buffer_size=16 * 1024 * 1024,
        )
        db = tidesdb.TidesDB(config)
        db.create_column_family("test_cf")
        cf = db.get_column_family("test_cf")

        with db.begin_txn() as txn:
            txn.put(cf, b"hello", b"world")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"hello") == b"world"

        db.close()


class TestObjectStoreConfigFields:
    """Tests for object store fields in ColumnFamilyConfig."""

    def test_cf_config_defaults(self):
        """Test object store config defaults in ColumnFamilyConfig."""
        config = tidesdb.ColumnFamilyConfig()
        assert config.object_target_file_size == 0
        assert config.object_lazy_compaction is False
        assert config.object_prefetch_compaction is True

    def test_cf_config_custom_values(self, db):
        """Test creating column family with object store config fields set."""
        config = tidesdb.ColumnFamilyConfig(
            object_target_file_size=128 * 1024 * 1024,
            object_lazy_compaction=True,
            object_prefetch_compaction=False,
        )
        db.create_column_family("obj_cf", config)
        cf = db.get_column_family("obj_cf")

        with db.begin_txn() as txn:
            txn.put(cf, b"key", b"value")
            txn.commit()

        with db.begin_txn() as txn:
            assert txn.get(cf, b"key") == b"value"

        db.drop_column_family("obj_cf")


class TestDbStatsNewFields:
    """Tests for new unified memtable and object store fields in DbStats."""

    def test_unified_memtable_stats_fields(self, db):
        """Test that unified memtable stats fields are present and typed correctly."""
        stats = db.get_db_stats()
        assert isinstance(stats.unified_memtable_enabled, bool)
        assert isinstance(stats.unified_memtable_bytes, int)
        assert isinstance(stats.unified_immutable_count, int)
        assert isinstance(stats.unified_is_flushing, bool)
        assert isinstance(stats.unified_next_cf_index, int)
        assert isinstance(stats.unified_wal_generation, int)

    def test_object_store_stats_fields(self, db):
        """Test that object store stats fields are present and typed correctly."""
        stats = db.get_db_stats()
        assert isinstance(stats.object_store_enabled, bool)
        assert isinstance(stats.object_store_connector, str)
        assert isinstance(stats.local_cache_bytes_used, int)
        assert isinstance(stats.local_cache_bytes_max, int)
        assert isinstance(stats.local_cache_num_files, int)
        assert isinstance(stats.last_uploaded_generation, int)
        assert isinstance(stats.upload_queue_depth, int)
        assert isinstance(stats.total_uploads, int)
        assert isinstance(stats.total_upload_failures, int)
        assert isinstance(stats.replica_mode, bool)

    def test_unified_memtable_stats_enabled(self, temp_db_path):
        """Test unified memtable stats when enabled."""
        db = tidesdb.TidesDB.open(
            temp_db_path,
            unified_memtable=True,
            unified_memtable_write_buffer_size=32 * 1024 * 1024,
            unified_memtable_skip_list_max_level=12,
            unified_memtable_skip_list_probability=0.25,
        )
        stats = db.get_db_stats()
        assert stats.unified_memtable_enabled is True
        db.close()

    def test_object_store_disabled_by_default(self, db):
        """Test that object store is disabled by default."""
        stats = db.get_db_stats()
        assert stats.object_store_enabled is False
        assert stats.replica_mode is False


class TestIteratorKeyValue:
    """Tests for the combined key_value() iterator method."""

    def test_key_value_basic(self, db, cf):
        """Test basic key_value() retrieval."""
        with db.begin_txn() as txn:
            txn.put(cf, b"a", b"val_a")
            txn.put(cf, b"b", b"val_b")
            txn.put(cf, b"c", b"val_c")
            txn.commit()

        with db.begin_txn() as txn:
            with txn.new_iterator(cf) as it:
                it.seek_to_first()
                assert it.valid()
                key, value = it.key_value()
                assert key == b"a"
                assert value == b"val_a"

    def test_key_value_iteration(self, db, cf):
        """Test using key_value() in iteration loop."""
        expected = {}
        with db.begin_txn() as txn:
            for i in range(5):
                k = f"iter_kv_{i:03d}".encode()
                v = f"value_{i}".encode()
                expected[k] = v
                txn.put(cf, k, v)
            txn.commit()

        with db.begin_txn() as txn:
            results = {}
            with txn.new_iterator(cf) as it:
                it.seek(b"iter_kv_")
                while it.valid():
                    key, value = it.key_value()
                    if not key.startswith(b"iter_kv_"):
                        break
                    results[key] = value
                    it.next()

            assert results == expected

    def test_key_value_via_iter_protocol(self, db, cf):
        """Test that __next__ uses key_value() internally."""
        with db.begin_txn() as txn:
            txn.put(cf, b"x", b"1")
            txn.put(cf, b"y", b"2")
            txn.commit()

        with db.begin_txn() as txn:
            with txn.new_iterator(cf) as it:
                it.seek_to_first()
                pairs = list(it)
                assert len(pairs) >= 2
                # Verify tuples are returned
                for k, v in pairs:
                    assert isinstance(k, bytes)
                    assert isinstance(v, bytes)


class TestPromoteToPrimary:
    """Tests for promote_to_primary method."""

    def test_promote_to_primary_without_object_store(self, db):
        """Test promote_to_primary on a non-replica db (should fail gracefully)."""
        with pytest.raises(tidesdb.TidesDBError):
            db.promote_to_primary()

    def test_promote_method_exists(self, db):
        """Test that promote_to_primary method is available."""
        assert hasattr(db, "promote_to_primary")
        assert callable(db.promote_to_primary)


class TestErrorCodes:
    """Tests for error codes."""

    def test_readonly_error_code_exists(self):
        """Test that TDB_ERR_READONLY is defined."""
        assert tidesdb.TDB_ERR_READONLY == -13

    def test_readonly_error_message(self):
        """Test that readonly error has proper message."""
        err = tidesdb.TidesDBError.from_code(tidesdb.TDB_ERR_READONLY)
        assert "read-only" in str(err)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
