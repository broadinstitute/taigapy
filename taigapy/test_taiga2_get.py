import os
import pytest

from taigapy import Taiga2Client as TaigaClient

token_path = os.path.expanduser("~/.taiga/token")


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
])
def test_is_valid_dataset(tmpdir, taigaClient, dataset, format, expected):
    assert taigaClient.is_valid_dataset(dataset, format=format) == expected


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
    summary = taigaClient.get_short_summary(name="calico-t1-log-viability-30de", version="1", file="pcal_t1sec_log2viab_dose2_2.5um")
    assert summary == '576x1119 matrix, 50911 NAs'


def test_get_short_summary_without_version(tmpdir, taigaClient: TaigaClient):
    summary = taigaClient.get_short_summary(name="calico-t1-log-viability-30de", file="pcal_t1sec_log2viab_dose2_2.5um")
    assert summary == '576x1119 matrix, 50911 NAs'


def test_no_pickle_hdf5(tmpdir):
    """
    :param tmpdir:
    :param taigaClient:
    :return:
    This test make sure pickling in cache is working only for CSV files. It should not pickle HDF5.
    """
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)

    # Test HDF5 leads to only one file
    local_file = taigaClient.download_to_cache(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7', format='hdf5')
    assert os.path.exists(local_file)
    assert get_cached_count(cache_dir) == 1


def test_no_pickle_hdf5(tmpdir):
    """
    :param tmpdir:
    :param taigaClient:
    :return:
    This test make sure pickling in cache is working for CSV files
    """
    cache_dir = str(tmpdir.join("cache"))
    taigaClient = TaigaClient(cache_dir=cache_dir, token_path=token_path)

    # Test CSV leads to two files (CSV file downloaded + pickle)
    local_file = taigaClient.download_to_cache(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7', format='csv')
    assert os.path.exists(local_file)
    assert get_cached_count(cache_dir) == 2