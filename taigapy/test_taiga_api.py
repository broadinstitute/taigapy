import pytest
from unittest.mock import create_autospec

import taigapy.taiga_api


@pytest.fixture
def MockTaigaApi(monkeypatch):
    mock_class = create_autospec(taigapy.taiga_api.TaigaApi)
    monkeypatch.setattr(taigapy.taiga_api, "TaigaApi", mock_class)
    return taigapy.taiga_api.TaigaApi


@pytest.mark.parametrize(
    "id_or_permaname,dataset_name,dataset_version,datafile_name", [("todo", 1, 1, 1)]
)
def test_get_dataset_metadata(
    MockTaigaApi, id_or_permaname, dataset_name, dataset_version, datafile_name
):
    MockTaigaApi.get_datafile_metadata(
        id_or_permaname, dataset_name, dataset_version, datafile_name
    )


def test_get_dataset_version_metadata(MockTaigaApi):
    MockTaigaApi.get_dataset_version_metadata


def test_upload_dataset(MockTaigaApi):
    MockTaigaApi.upload_dataset


def test_poll_task(MockTaigaApi):
    MockTaigaApi.poll_task


def test_get_column_types(MockTaigaApi):
    MockTaigaApi.get_column_types


def test_download_datafile(MockTaigaApi):
    MockTaigaApi.download_datafile
