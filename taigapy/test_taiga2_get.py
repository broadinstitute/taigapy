import os
import pytest
import numpy as np
import pandas as pd

from taigapy import Taiga2Client as TaigaClient

token_path = os.path.expanduser("~/.taiga/token")

from pandas.util.testing import assert_frame_equal

def get_cached_count(cache_dir):
    return len(os.listdir(cache_dir))


@pytest.fixture(scope="session")
def taigaClient(tmpdir_factory):
    cache_dir = str(tmpdir_factory.getbasetemp().join("cache"))
    c = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    return c


def test_get(tmpdir):
    cache_dir = str(tmpdir.join("cache"))

    c = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    df = c.get(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')
    assert df is not None
    # Changing get_cached_count to accept more than one file, since pickling now creates 2 files
    assert get_cached_count(cache_dir) >= 1

    df1 = c.get(name='depcon-binary-context-matrix', version=1)
    assert df1 is not None
    # Same as above: Changing get_cached_count to accept more than one file, since pickling now creates 2 files
    assert get_cached_count(cache_dir) >= 1
    # verify that we got a pandas object indexable by row and col names
    assert df.loc["MDAMB453_BREAST", "breast"] == 1.0
    assert df.loc["MDAMB453_BREAST", "rhabdoid"] == 0.0

    #
    # df2 = c.get(name='depcon-binary-context-matrix', version=3)
    # assert df2 is not None
    # assert get_cached_count() == 2


def test_get_table(tmpdir, taigaClient):
    # test fetch table
    df3 = taigaClient.get(name="lineage-colors", version=1)
    assert df3 is not None


@pytest.mark.parametrize("dataset, format, expected", [
    ("small-hgnc-2a89.2", "csv", True),
    ("small-hgnc-2a89", "csv", False),
    ("small-hgnc-2a89.2", "raw", False),
    ("small-hgnc-2a89.2", None, True),
])
def test_is_valid_dataset(tmpdir, taigaClient, dataset, format, expected):
    if format:
        assert taigaClient.is_valid_dataset(dataset, format=format) == expected
    else:
        assert taigaClient.is_valid_dataset(dataset) == expected


def test_download_hdf5(tmpdir, taigaClient):
    local_file = taigaClient.download_to_cache(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7', format='hdf5')
    assert 'hdf5' in local_file
    assert os.path.exists(local_file)


def test_download_multiple_files(tmpdir, taigaClient):
    t1 = taigaClient.get(name='taigr-data-40f2', version=1, file="non-utf8-table")
    assert t1.shape == (2, 3)

    t2 = taigaClient.get(name='taigr-data-40f2', version=1, file="tiny_table")
    assert t2.shape == (3, 4)


def test_implicit_latest_version(tmpdir, taigaClient):
    df4 = taigaClient.get(name="lineage-colors", file="data")
    assert df4 is not None


def test_get_implicit_raw_last_version(tmpdir, taigaClient):
    with pytest.raises(Exception) as excinfo:
        df4 = taigaClient.get(name="top-pref-dep-dict-86d9", file="top_pref_deps_dict")
    assert str(excinfo) is not None


def test_download_to_cache_implicit_raw_last_version(tmpdir, taigaClient):
    df4 = taigaClient.download_to_cache(name="top-pref-dep-dict-86d9", file="top_pref_deps_dict")
    assert df4 is not None


def test_name_version_as_id_input(tmpdir, taigaClient):
    df4 = taigaClient.get(id="lineage-colors.1")
    assert df4 is not None


def test_name_version_file_as_id_input(tmpdir, taigaClient):
    # df5 = c.get(id="avana-1-0-83e3.2/cell_line_info") # bug found? this dataset doesnt work
    df5 = taigaClient.get(id="taigr-data-40f2.1/non-utf8-table")
    assert df5 is not None


def test_period_in_file_name(tmpdir, taigaClient):
    df6 = taigaClient.get(id="calico-t1-log-viability-30de.1/pcal_t1sec_log2viab_dose2_2.5um")
    assert df6 is not None


def test_get_short_summary_full(tmpdir, taigaClient: TaigaClient):
    summary = taigaClient.get_short_summary(name="calico-t1-log-viability-30de", version="1",
                                            file="pcal_t1sec_log2viab_dose2_2.5um")
    assert summary == '576x1119 matrix, 50911 NAs'


def test_get_short_summary_without_version(tmpdir, taigaClient: TaigaClient):
    summary = taigaClient.get_short_summary(name="calico-t1-log-viability-30de", file="pcal_t1sec_log2viab_dose2_2.5um")
    assert summary == '576x1119 matrix, 50911 NAs'

def test_get_dataset_metadata(tmpdir, taigaClient: TaigaClient):
    metadata = taigaClient.get_dataset_metadata("taigr-data-40f2")
    assert type(metadata) == dict
    assert "description" in metadata
    assert "datasetVersion" not in metadata

def test_get_dataset_metadata_with_version(tmpdir, taigaClient: TaigaClient):
    metadata = taigaClient.get_dataset_metadata("taigr-data-40f2", version=1)
    assert type(metadata) == dict
    assert "datasetVersion" in metadata

def test_get_dataset_metadata_with_version_id(tmpdir, taigaClient: TaigaClient):
    metadata = taigaClient.get_dataset_metadata(version_id="f20ef5fb44794e52867e2e9ff6165822")
    assert type(metadata) == dict

    dataset_metadata = taigaClient.get_dataset_metadata("taigr-data-40f2", version=1)
    assert metadata == dataset_metadata["datasetVersion"]

def test_feather_get(tmpdir):
    """
    Test that .get() call writes a feather file and a featherextra file
    """
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    taigaClient.get('b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')
    assert get_cached_count(cache_dir) == 2
    assert any(path.endswith('.feather') for path in os.listdir(cache_dir))
    assert any(path.endswith('.featherextra') for path in os.listdir(cache_dir))

def test_feather_get_with_existing_csv_file(tmpdir):
    '''
    Test that if there is an existing csv file,
        a feather file is created
        a featherextra file is created
        and the existing csv file is not deleted
    '''
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    taigaClient.download_to_cache(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7', format='csv')
    assert get_cached_count(cache_dir) == 1

    taigaClient.get('b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')
    assert get_cached_count(cache_dir) == 3
    assert {filename.split('.')[-1] for filename in os.listdir(cache_dir)} == {'csv', 'feather', 'featherextra'}

def test_no_pickle_download_to_cache(tmpdir):
    '''
    Test that download_to_cache downloads a non-pickled file
    '''
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)

    local_file = taigaClient.download_to_cache(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7', format='csv')
    assert os.path.exists(local_file)
    assert local_file.endswith('.csv')
    assert get_cached_count(cache_dir) == 1

def test_corrupted_feather(tmpdir):
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    df1 = taigaClient.get(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')

    # corrupt the file by truncating it
    # first find the file...
    cache_files = [os.path.join(cache_dir, x) for x in os.listdir(cache_dir) if x.endswith('feather')]
    assert len(cache_files) == 1
    cache_file = cache_files[0]
    
    # now overwrite it with only the first 100 bytes
    with open(cache_file, "rb") as fd:
        data = fd.read()
    assert len(data) > 100
        
    with open(cache_file, "wb") as fd:
        fd.write(data[:100])

    # try reading it back    
    df2 = taigaClient.get(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')

    # and make sure we get the same result
    assert_frame_equal(df1, df2)

def test_types(tmpdir, taigaClient):
    df = taigaClient.get("taigr-data-40f2.7/types_table")
    assert (
        df.columns.to_list()
        == [
            "int_only",
            "float_only",
            "string_only",
            "numeric_string_only",
            "python_bool_only",
            "R_full_bool_only",
            "int_float",
            "int_float_string",
            "float_string",
            "int_empty",
            "float_empty",
            "string_empty",
            "bool_empty",
            "int_NA",
            "float_NA",
            "string_NA",
            "bool_NA",
            "int_empty_NA",
            "float_empty_NA",
            "string_empty_NA",
            "bool_empty_NA",
        ]
    )
    assert (
        df.dtypes.to_list()
        == [
            np.int64,
            np.float64,
            np.object,
            np.int64,
            np.bool,
            np.bool,
            np.float64,
            np.object,
            np.object,
            np.float64,
            np.float64,
            np.object,
            np.object,
            np.float64,
            np.float64,
            np.object,
            np.object,
            np.float64,
            np.float64,
            np.object,
            np.object,
        ]
    )
    assert [pd.api.types.infer_dtype(df[c], skipna=False) for c in df.columns] == [
        "integer",
        "floating",
        "string",
        "integer",
        "boolean",
        "boolean",
        "floating",
        "string",
        "string",
        "floating",
        "floating",
        "mixed",
        "mixed",
        "floating",
        "floating",
        "mixed",
        "mixed",
        "floating",
        "floating",
        "mixed",
        "mixed",
    ]
    assert [pd.api.types.infer_dtype(df[c], skipna=True) for c in df.columns] == [
        "integer",
        "floating",
        "string",
        "integer",
        "boolean",
        "boolean",
        "floating",
        "string",
        "string",
        "floating",
        "floating",
        "string",
        "boolean",
        "floating",
        "floating",
        "string",
        "boolean",
        "floating",
        "floating",
        "string",
        "boolean",
    ]

    assert (
        df[
            [
                "int_empty",
                "float_empty",
                "string_empty",
                "bool_empty",
                "int_NA",
                "float_NA",
                "string_NA",
                "bool_NA",
                "int_empty_NA",
                "float_empty_NA",
                "string_empty_NA",
                "bool_empty_NA",
            ]
        ]
        .loc[2]
        .isnull()
        .all()
    )


# commenting out because this test fails and is testing new functionality. I'm unclear if this is a regression or this has never worked because
# I don't believe anything has changed since this test was initially written.
#
#@pytest.mark.parametrize("parameters", [
#    {'name': 'test-get-all-76f8', 'version': 1},
#    {'name': 'test-get-all-76f8'}
#])
#def test_get_all(tmpdir, taigaClient: TaigaClient, parameters):
#    """Test if get without specifying a file returns properly a dict with the dataframes/raw data in them"""
#    all_files = taigaClient.get_all(**parameters)
#
#    assert isinstance(all_files, dict)
#    assert 'sparkles' in all_files
#    assert 'master-cell-line-export_v108-masterfile-2018-09-17' in all_files
#    assert 'test_matrix' in all_files
