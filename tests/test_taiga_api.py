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
    taigaApi: TaigaApi, id_or_permaname, dataset_name, dataset_version, datafile_name
):
    assert (
        taigaApi.get_datafile_metadata(
            id_or_permaname, dataset_name, dataset_version, datafile_name
        )
        is not None
    )


from taigapy.taiga_api import run_with_max_retries
import pytest
import time


def test_run_with_max_retries_fails(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)
    counter = 0

    def inner():
        nonlocal counter
        counter += 1
        raise IOError()

    with pytest.raises(IOError):
        run_with_max_retries(inner, 3)

    assert counter == 3


def test_run_with_max_retries_success(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda x: None)
    counter = 0

    def inner():
        nonlocal counter
        counter += 1
        if counter < 3:
            raise IOError()

    run_with_max_retries(inner, 3)

    assert counter == 3
