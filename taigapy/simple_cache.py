from typing import TypeVar, Generic, Optional, Callable
import pickle

import sqliteshelve as shelve
import os

V = TypeVar("V")

from contextlib import contextmanager
from typing import Dict

@contextmanager
def shelve_open(filename):
    d = shelve.open(filename)
    yield d
    d.close()
from datetime import datetime
class Cache(Generic[V]):
    """
    A write-through in-memory cache, backed by disk using SQLite.

    Keys must always be strings, but values can be any pickle-able type.

    The cache uses `sqliteshelve` for persistent storage, which stores data in a SQLite database.
    This allows for efficient storage and retrieval of cached values.

    The cache also maintains an in-memory dictionary to provide fast access to frequently used values.
    When a value is requested, the cache first checks if it exists in the in-memory dictionary.
    If not, it retrieves the value from the SQLite database and stores it in the in-memory dictionary.

    The cache also supports a validity check for cached values. A `is_value_valid` function can be provided
    to determine if a cached value is still valid. If a cached value is not valid, it will be treated as
    if it does not exist in the cache, and the `default` value will be returned.

    Attributes:
        filename (str): The path to the SQLite database file used for persistent storage.
        is_value_valid (Callable[[V], bool]): A function that takes a cached value as input and returns
            True if the value is still valid, False otherwise. Defaults to a function that always returns True.
        track_last_access (bool): Whether to track the last access time for each key. Defaults to False.
    """

    def __init__(
        self, filename: str, is_value_valid: Callable[[V], bool] = lambda value: True, track_last_access:bool = False
    ) -> None:
        super().__init__()
        self.in_memory_cache = {}
        self.filename = filename
        self.track_last_access = track_last_access
        self.is_value_valid = is_value_valid

    def _ensure_parent_dir_exists(self):
        parent = os.path.dirname(self.filename)
        if not os.path.exists(parent):
            os.makedirs(parent)

    def _none_if_not_valid(self, value: V, default) -> Optional[V]:
        if self.is_value_valid(value):
            return value
        else:
            return default

    def _update_last_access(self, key):
        with self._open_last_access_shelve() as s:
            s[key] = datetime.now()

    def get(self, key: str, default: Optional[V]) -> Optional[V]:
        """
        Retrieves a value from the cache.

        Args:
            key (str): The key of the value to retrieve.
            default (Optional[V]): The value to return if the key is not found in the cache or if the cached value is not valid.

        Returns:
            Optional[V]: The cached value if found and valid, otherwise the default value.
        """
        if key in self.in_memory_cache:
            value = self._none_if_not_valid(self.in_memory_cache[key], default)
        else:
            self._ensure_parent_dir_exists()
            with shelve_open(self.filename) as s:
                if key in s:
                    # If we have a value we
                    # cannot reconstruct, consider that as
                    # the same as missing and return `default`
                    try:
                        value = s[key]
                    except pickle.UnpicklingError as ex:
                        print(f"Warning: Got error trying to unpickle object: {ex}")
                        value = default

                    if value is not default:
                        value = self._none_if_not_valid(value, default)

                    if value is not default:
                        self.in_memory_cache[key] = value
                else:
                    value = default

        if value is not default:
            self._update_last_access(key)
        return value

    def get_last_access_per_key(self) -> Dict[str, Optional[datetime]]:
        assert self.track_last_access
        snapshot = dict()
        # add all keys with None as the timestamp in case last access dict is missing some
        with shelve_open(self.filename) as s:
            for key in s.keys():
                snapshot[key] = None

        with self._open_last_access_shelve() as s:
            snapshot.update(s)

        return snapshot

    def _open_last_access_shelve(self):
        return shelve_open(f"{self.filename}-last-access")

    def put(self, key: str, value: V):
        """
        Stores a value in the cache.

        Args:
            key (str): The key of the value to store.
            value (V): The value to store.
        """
        assert self.is_value_valid(value)
        self._ensure_parent_dir_exists()
        with shelve_open(self.filename) as s:
            s[key] = value
            self.in_memory_cache[key] = value
        self._update_last_access(key)
