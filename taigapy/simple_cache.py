from typing import TypeVar, Generic, Optional, Callable
import pickle

import sqliteshelve as shelve
import os

V = TypeVar("V")


class Cache(Generic[V]):
    """
    A write through in-memory cache, backed by disk. Keys must always be strings, but values can be any
    pickle-able type.
    """

    def __init__(
        self, filename: str, is_value_valid: Callable[[V], bool] = lambda value: True
    ) -> None:
        super().__init__()
        self.in_memory_cache = {}
        self.filename = filename
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

    def get(self, key: str, default: Optional[V]) -> Optional[V]:
        if key in self.in_memory_cache:
            return self._none_if_not_valid(self.in_memory_cache[key], default)

        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            if key in s:
                # If we have a value we
                # cannot reconstruct, consider that as
                # the same as missing and return `default`
                try:
                    value = s[key]
                except pickle.UnpicklingError as ex:
                    print(f"Warning: Got error trying to unpickle object: {ex}")
                    return default

                self.in_memory_cache[key] = value
                return self._none_if_not_valid(value, default)
            else:
                return default

    def put(self, key: str, value: V):
        assert self.is_value_valid(value)
        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            s[key] = value
            self.in_memory_cache[key] = value
