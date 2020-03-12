import os
import py
import pytest

from taigapy.taiga_cache import TaigaCache, DataFile
from taigapy import DataFileType, CACHE_FILE

FULL_TAIGA_ID = "some-dataset.1/some-file"
TAIGA_ID_ALIAS = "some-dataset.1"
VIRTUAL_TAIGA_ID = "some-dataset.2/some-file"
FEATHER_PATH = FULL_TAIGA_ID.replace(".", "/") + ".feather"


@pytest.fixture
def populated_cache(tmpdir: py._path.local.LocalPath):
    cache = TaigaCache(str(tmpdir.join(CACHE_FILE)))

    cache.add_entry(FULL_TAIGA_ID, FEATHER_PATH, DataFileType.HDF5.value)
    cache.add_alias(TAIGA_ID_ALIAS, FULL_TAIGA_ID)
    cache.add_virtual_datafile(VIRTUAL_TAIGA_ID, FULL_TAIGA_ID)

    return cache


def test_get_entry(populated_cache: TaigaCache):
    expected_datafile = DataFile(
        FULL_TAIGA_ID, None, FEATHER_PATH, DataFileType.HDF5.value
    )
    real_file = populated_cache.get_entry(FULL_TAIGA_ID)
    assert expected_datafile == real_file

    aliased_file = populated_cache.get_entry(TAIGA_ID_ALIAS)
    assert expected_datafile == real_file

    virtual_file = populated_cache.get_entry(VIRTUAL_TAIGA_ID)
    assert expected_datafile == real_file
