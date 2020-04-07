import pdb

import os
import pandas as pd
import py
import pytest
from typing import Mapping, Optional
from unittest.mock import patch

from taigapy.taiga_cache import TaigaCache, DataFile

# TODO: Fix once big bang version is finished 🙃
from taigapy.__init_big_bang__ import CACHE_FILE
from taigapy.types import DataFileType, DataFileFormat

COLUMNAR_DATAFRAME = pd.DataFrame(
    {"foo": [1.0, 2.0, 3.0], "bar": ["four", "five", "six"], "baz": [7.0, 8.0, 9.0]}
)
COLUMNAR_TYPES = {"foo": "float", "bar": "str", "baz": "float"}
COLUMNAR_FULL_TAIGA_ID = "columnar-dataset.1/some-file"
COLUMNAR_TAIGA_ID_ALIAS = "columnar-dataset.1"
COLUMNAR_VIRTUAL_TAIGA_ID = "columnar-dataset.2/some-file"

MATRIX_DATAFRAME = pd.DataFrame(
    {"foo": [1, 2, 3], "bar": [4, 5, 6], "baz": [7, 8, 9]},
    index=["wibble", "wobble", "wubble"],
    dtype=float,
)
MATRIX_FULL_TAIGA_ID = "matrix-dataset.1/some-file"
MATRIX_TAIGA_ID_ALIAS = "matrix-dataset.1"
MATRIX_VIRTUAL_TAIGA_ID = "matrix-dataset.2/some-file"


@pytest.mark.parametrize(
    "full_taiga_id,taiga_id_alias,virtual_taiga_id,datafile_format,column_types",
    [
        (
            COLUMNAR_FULL_TAIGA_ID,
            COLUMNAR_TAIGA_ID_ALIAS,
            COLUMNAR_VIRTUAL_TAIGA_ID,
            DataFileFormat.Columnar,
            COLUMNAR_TYPES,
        ),
        (
            MATRIX_FULL_TAIGA_ID,
            MATRIX_TAIGA_ID_ALIAS,
            MATRIX_VIRTUAL_TAIGA_ID,
            DataFileFormat.HDF5,
            None,
        ),
    ],
)
def test_add_entry(
    populated_cache: TaigaCache,
    full_taiga_id: str,
    taiga_id_alias: str,
    virtual_taiga_id: str,
    datafile_format: DataFileFormat,
    column_types: Optional[Mapping[str, str]],
):
    with patch(
        "taigapy.taiga_cache._write_csv_to_feather"
    ) as mock_write_csv_to_feather:
        populated_cache.add_entry(
            None, full_taiga_id, full_taiga_id, datafile_format, column_types, None
        )
        assert not mock_write_csv_to_feather.called

        populated_cache.add_entry(
            None, taiga_id_alias, full_taiga_id, datafile_format, column_types, None
        )
        assert not mock_write_csv_to_feather.called

        populated_cache.add_entry(
            None, virtual_taiga_id, full_taiga_id, datafile_format, column_types, None
        )
        assert not mock_write_csv_to_feather.called


@pytest.fixture
def populated_cache(tmpdir: py._path.local.LocalPath):
    cache = TaigaCache(str(tmpdir), str(tmpdir.join(CACHE_FILE)))

    p = tmpdir.join("foobar.csv")
    COLUMNAR_DATAFRAME.to_csv(p, index=False),

    cache.add_entry(
        str(p),
        COLUMNAR_FULL_TAIGA_ID,
        COLUMNAR_FULL_TAIGA_ID,
        DataFileFormat.Columnar,
        COLUMNAR_TYPES,
        None,
    )

    MATRIX_DATAFRAME.astype(float).to_csv(p)
    cache.add_entry(
        str(p),
        MATRIX_FULL_TAIGA_ID,
        MATRIX_FULL_TAIGA_ID,
        DataFileFormat.HDF5,
        None,
        None,
    )

    return cache


@pytest.mark.parametrize(
    "expected_df,full_taiga_id,taiga_id_alias,virtual_taiga_id",
    [
        (
            COLUMNAR_DATAFRAME,
            COLUMNAR_FULL_TAIGA_ID,
            COLUMNAR_TAIGA_ID_ALIAS,
            COLUMNAR_VIRTUAL_TAIGA_ID,
        ),
        (
            MATRIX_DATAFRAME,
            MATRIX_FULL_TAIGA_ID,
            MATRIX_TAIGA_ID_ALIAS,
            MATRIX_VIRTUAL_TAIGA_ID,
        ),
    ],
)
def test_get_entry(
    populated_cache: TaigaCache,
    expected_df: pd.DataFrame,
    full_taiga_id: str,
    taiga_id_alias: str,
    virtual_taiga_id: str,
):
    """
    TODO:
    - get full_taiga_id should return the df
    - get virtual taiga id should not return the df until the link has been added
        - then add virtual taiga id (need to figure out parameters)
        - get virtual taiga id should return df
        - same for alias
    - removing the actual file should remove the entry just for the actual taiga id,
      but not the aliases/virtual taiga id
    """
    df = populated_cache.get_entry(full_taiga_id, full_taiga_id)
    assert df.equals(expected_df)

    df_alias = populated_cache.get_entry(taiga_id_alias, full_taiga_id)
    assert df_alias.equals(df)

    df_virtual = populated_cache.get_entry(virtual_taiga_id, full_taiga_id)
    assert df_virtual.equals(df)


def test_get_raw_entry(tmpdir, populated_cache: TaigaCache):
    p = tmpdir.join("foobar.txt")
    with open(str(p), "w+") as f:
        f.write("baz")

    populated_cache.add_raw_entry(
        str(p), "raw-dataset.1", "raw-dataset.1/some-file", DataFileFormat.Raw
    )

    path_from_cache = populated_cache.get_raw_path(
        "raw-dataset.1", "raw-dataset.1/some-file"
    )
    with open(path_from_cache) as f:
        assert f.read() == "baz"


def test_remove_from_cache(populated_cache: TaigaCache):
    populated_cache.remove_from_cache(COLUMNAR_FULL_TAIGA_ID, COLUMNAR_FULL_TAIGA_ID)

    assert (
        populated_cache.get_entry(COLUMNAR_FULL_TAIGA_ID, COLUMNAR_FULL_TAIGA_ID)
        is None
    )
    assert populated_cache.get_entry(COLUMNAR_TAIGA_ID_ALIAS, COLUMNAR_FULL_TAIGA_ID) is None
    
    assert (
        populated_cache.get_entry(COLUMNAR_VIRTUAL_TAIGA_ID, COLUMNAR_FULL_TAIGA_ID)
        is None
    )

    c = populated_cache.conn.cursor()
    c.execute(
        """
        SELECT * FROM datafiles
        """
    )
    rows = c.fetchall()
    assert len(rows) == 1


def test_remove_all_from_cache(populated_cache: TaigaCache):
    populated_cache.remove_all_from_cache
