from typing import TypeVar, Generic, Optional

import shelve # TODO: Switch away from shelve to sqlite shelve to avoid concurrency corruption
import os

V = TypeVar("V")
class Cache(Generic[V]):
    """A write through in-memory cache, backed by disk"""
    def __init__(self, filename: str) -> None:
        super().__init__()
        self.in_memory_cache = {}
        self.filename = filename

    def _ensure_parent_dir_exists(self):
        parent = os.path.dirname(self.filename)
        if not os.path.exists(parent):
            os.makedirs(parent)

    def get(self, key: str, default: Optional[V]) -> V:
        if key in self.in_memory_cache:
            return self.in_memory_cache[key]

        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            if key in s:
                value = s[key]
                self.in_memory_cache[key] = value
                return value
            else:
                return default

    def put(self, key: str, value: V):
        self._ensure_parent_dir_exists()
        with shelve.open(self.filename) as s:
            if key in s:
                assert s[key] == value
            else:
                s[key] = value
                self.in_memory_cache[key] = value
