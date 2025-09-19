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
    A write through in-memory cache, backed by disk. Keys must always be strings, but values can be any
    pickle-able type.
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
        with shelve_open(f"{self.filename}-last-access") as s:
            s[key] = datetime.now()

    def get(self, key: str, default: Optional[V]) -> Optional[V]:
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

    def get_last_access_per_key(self) -> Dict[str, datetime]:
        assert self.track_last_access
        snapshot = dict()
        with shelve_open(f"{self.filename}-last-access") as s:
            s.update(snapshot)
        return snapshot

    def put(self, key: str, value: V):
        assert self.is_value_valid(value)
        self._ensure_parent_dir_exists()
        with shelve_open(self.filename) as s:
            s[key] = value
            self.in_memory_cache[key] = value
        self._update_last_access(key)
