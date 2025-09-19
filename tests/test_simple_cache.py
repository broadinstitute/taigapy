import os
import shutil
from taigapy.simple_cache import Cache
from datetime import datetime, timedelta
import time
import pytest


@pytest.fixture
def cache(tmpdir):
    cache_dir = str(tmpdir)
    cache_file = os.path.join(cache_dir, "test_cache.db")
    os.makedirs(cache_dir, exist_ok=True)
    cache = Cache(cache_file)
    yield cache
    shutil.rmtree(cache_dir)


def test_basic_get_put(cache):
    cache.put("key1", "value1")
    assert cache.get("key1", None) == "value1"


def test_cache_hit_and_miss(cache):
    cache.put("key2", "value2")
    assert cache.get("key2", None) == "value2"  # Cache hit
    assert cache.get("key3", "default") == "default"  # Cache miss


def test_value_validity_check(cache, tmpdir):
    sample_file = tmpdir.join("sample")
    sample_file.write("")

    def is_valid(value):
        return os.path.exists(value)

    cache = Cache(cache.filename, is_value_valid=is_valid)

    cache.put("key4", str(sample_file))
    assert cache.get("key4", None) == str(sample_file)

    os.unlink(str(sample_file))
    assert cache.get("key4", "default") == "default"


def test_last_access_tracking(cache):
    cache = Cache(cache.filename, track_last_access=True)
    cache.put("key6", "value6")
    last_access = cache.get_last_access_per_key()
    assert "key6" in last_access
    orig_access_time = last_access["key6"]
    assert isinstance(orig_access_time, datetime)

    time.sleep(0.0000001)  # Ensure some time passes

    cache.get("key6", None)
    last_access = cache.get_last_access_per_key()
    assert last_access["key6"] > orig_access_time

