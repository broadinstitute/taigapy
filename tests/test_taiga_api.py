import os
import pytest

from taigapy.taiga_api import TaigaApi
from taigapy.utils import format_datafile_id

DATASET_VERSION_ID = "89d374e19eaf4358b2a467cacd966e93"
DATASET_PERMANAME = "depcon-binary-context-matrix"
DATASET_VERSION = 3
DATAFILE_NAME = "depcon_binary_context_matrix"


@pytest.fixture
def taigaApi():
    with open(os.path.abspath(os.path.expanduser("~/.taiga/token")), "rt") as r:
        token = r.readline().strip()

    return TaigaApi("https://cds.team/taiga", token)


@pytest.mark.parametrize(
    "id_or_permaname,dataset_name,dataset_version,datafile_name",
    [
        (
            format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None),
            None,
            None,
            DATAFILE_NAME,
        ),
        (None, DATASET_PERMANAME, DATASET_VERSION, DATAFILE_NAME),
        (None, DATASET_PERMANAME, DATASET_VERSION, None),
        (DATASET_VERSION_ID, None, None, DATAFILE_NAME),
        (DATASET_VERSION_ID, None, None, None),
    ],
)
def test_get_datafile_metadata(
    taigaApi: TaigaApi, id_or_permaname, dataset_name, dataset_version, datafile_name,
):
    assert (
        taigaApi.get_datafile_metadata(
            id_or_permaname, dataset_name, dataset_version, datafile_name
        )
        is not None
    )


def test_get_dataset_version_metadata(taigaApi: TaigaApi):
    """TODO"""
    taigaApi.get_dataset_version_metadata


def test_poll_task(taigaApi: TaigaApi):
    """TODO"""
    taigaApi._poll_task


def test_get_column_types(taigaApi: TaigaApi):
    """TODO"""
    taigaApi.get_column_types


def test_download_datafile(taigaApi: TaigaApi):
    """TODO"""
    taigaApi.download_datafile
