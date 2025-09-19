import os
import shutil
from taigapy.simple_cache import Cache
from datetime import datetime, timedelta
import time
import pytest


@pytest.fixture
def cache():
    cache_dir = "test_cache"
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


def test_value_validity_check(cache):
    def is_valid(value):
        return value > 0

    cache = Cache(cache.filename, is_value_valid=is_valid)
    cache.put("key4", 10)
    assert cache.get("key4", None) == 10

    cache.put("key5", -5)
    assert cache.get("key5", "default") == "default"


def test_last_access_tracking(cache):
    cache = Cache(cache.filename, track_last_access=True)
    cache.put("key6", "value6")
    time.sleep(0.1)  # Ensure some time passes
    cache.get("key6", None)
    last_access = cache.get_last_access_per_key()
    assert "key6" in last_access
    assert isinstance(last_access["key6"], datetime)
    # Check that the last access time is recent
    assert datetime.now() - last_access["key6"] < timedelta(seconds=1)


def test_unpickling_error(cache):
    cache_file = cache.filename
    # Create a cache file with invalid data
    with open(cache_file, "wb") as f:
        f.write(b"invalid data")

    # Test that the cache handles the UnpicklingError gracefully
    cache = Cache(cache_file)
    assert cache.get("key7", "default") == "default"
