import unittest
import os
import shutil
from taigapy.simple_cache import Cache
from datetime import datetime, timedelta
import time

class TestSimpleCache(unittest.TestCase):
    def setUp(self):
        self.cache_dir = "test_cache"
        self.cache_file = os.path.join(self.cache_dir, "test_cache.db")
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache = Cache(self.cache_file)

    def tearDown(self):
        shutil.rmtree(self.cache_dir)

    def test_basic_get_put(self):
        self.cache.put("key1", "value1")
        self.assertEqual(self.cache.get("key1", None), "value1")

    def test_cache_hit_and_miss(self):
        self.cache.put("key2", "value2")
        self.assertEqual(self.cache.get("key2", None), "value2")  # Cache hit
        self.assertEqual(self.cache.get("key3", "default"), "default")  # Cache miss

    def test_value_validity_check(self):
        def is_valid(value):
            return value > 0

        cache = Cache(self.cache_file, is_value_valid=is_valid)
        cache.put("key4", 10)
        self.assertEqual(cache.get("key4", None), 10)

        cache.put("key5", -5)
        self.assertEqual(cache.get("key5", "default"), "default")

    def test_last_access_tracking(self):
        cache = Cache(self.cache_file, track_last_access=True)
        cache.put("key6", "value6")
        time.sleep(0.1)  # Ensure some time passes
        cache.get("key6", None)
        last_access = cache.get_last_access_per_key()
        self.assertIn("key6", last_access)
        self.assertIsInstance(last_access["key6"], datetime)
        # Check that the last access time is recent
        self.assertTrue(datetime.now() - last_access["key6"] < timedelta(seconds=1))

    def test_unpickling_error(self):
        # Create a cache file with invalid data
        with open(self.cache_file, "wb") as f:
            f.write(b"invalid data")

        # Test that the cache handles the UnpicklingError gracefully
        cache = Cache(self.cache_file)
        self.assertEqual(cache.get("key7", "default"), "default")

if __name__ == "__main__":
    unittest.main()
