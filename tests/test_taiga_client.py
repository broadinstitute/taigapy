import os
import pdb
import pytest
from unittest.mock import patch

import pandas as pd

# TODO: Change once replaced
from taigapy.__init_big_bang__ import TaigaClient
import taigapy.taiga_api
import taigapy.utils
from taigapy.utils import format_datafile_id

from taigapy.types import DatasetMetadataDict, DatasetVersionMetadataDict


DATASET_PERMANAME = "depcon-binary-context-matrix"
DATASET_VERSION = 3
DATAFILE_NAME = "depcon_binary_context_matrix"
DATAFILE_ID = format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, DATAFILE_NAME)


@pytest.fixture
def taigaClient(tmpdir):
    cache_dir = str(tmpdir.join("cache"))

    tc = TaigaClient(cache_dir=cache_dir, token_path="~/.taiga/token")
    return tc


@pytest.fixture
def populatedTaigaClient(taigaClient: TaigaClient):
    taigaClient.get(DATAFILE_ID)
    return taigaClient


@pytest.fixture
def localTaigaClient(tmpdir):
    cache_dir = tmpdir.join("cache")
    token_path = cache_dir.join("token")
    os.makedirs(cache_dir, exist_ok=True)
    with open(str(token_path), "w+") as f:
        f.write("test-token")

    tc = TaigaClient(
        url="http://localhost:5000/taiga",
        cache_dir=str(cache_dir),
        token_path=str(token_path),
    )
    return tc


class TestInit:
    def test_init(self, taigaClient):
        """
        TaigaClient.api should be None until a user-facing function is called. After
        which, token and api initialization should not run.
        """
        assert taigaClient.api is None
        taigaClient.get(DATAFILE_ID)
        assert taigaClient.api is not None
        with patch("taigapy.utils.format_datafile_id") as mock_format_datafile_id:
            taigaClient.get(DATAFILE_ID)
            assert not mock_format_datafile_id.called

    def test_init_nonexistent_token(self, tmpdir, capsys):
        """
        TaigaClient should error if token does not exist only when a user-facing
        function is called.
        """
        cache_dir = str(tmpdir.join("cache"))
        tc = TaigaClient(cache_dir=cache_dir, token_path="fake token path")

        assert tc.get(DATAFILE_ID) is None
        out, err = capsys.readouterr()
        assert out.startswith("No token file found.")


class TestGet:
    def test_get(self, taigaClient: TaigaClient):
        df = taigaClient.get(DATAFILE_ID)
        assert df is not None
        assert df.loc["MDAMB453_BREAST", "breast"] == 1.0
        assert df.loc["MDAMB453_BREAST", "rhabdoid"] == 0.0

        with patch(
            "taigapy.taiga_api.TaigaApi.download_datafile"
        ) as mock_download_datafile:
            df2 = taigaClient.get(DATAFILE_ID)
            assert not mock_download_datafile.called
            assert df2.equals(df)

    def test_get_input_formats(self, populatedTaigaClient: TaigaClient):
        with patch(
            "taigapy.taiga_api.TaigaApi.download_datafile"
        ) as mock_download_datafile:
            df = populatedTaigaClient.get(DATAFILE_ID)

            # Datafile ID without file name
            df2 = populatedTaigaClient.get(
                format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None)
            )
            assert not mock_download_datafile.called
            assert df2.equals(df)

            # Dataset name, version, datafile id entered separately
            df3 = populatedTaigaClient.get(
                name=DATASET_PERMANAME, version=DATASET_VERSION, file=DATAFILE_NAME
            )
            assert not mock_download_datafile.called
            assert df3.equals(df)

            # Dataset name, version entered separately, no datafile name
            df4 = populatedTaigaClient.get(
                name=DATASET_PERMANAME, version=DATASET_VERSION
            )
            assert not mock_download_datafile.called
            assert df4.equals(df)

            # Dataset name, datafile name entered separately, no dataset version
            df5 = populatedTaigaClient.get(name=DATASET_PERMANAME, file=DATAFILE_NAME)
            assert not mock_download_datafile.called
            assert df5.equals(df)

    def test_get_virtual(self, taigaClient: TaigaClient):
        df = taigaClient.get("beat-aml-5d92.14/cPCA_cell_loadings")
        with patch(
            "taigapy.taiga_api.TaigaApi.download_datafile"
        ) as mock_download_datafile:
            df2 = taigaClient.get("beat-aml-5d92.17/cPCA_cell_loadings")
            assert not mock_download_datafile.called
            assert df2.equals(df)

    def test_corrupted_feather(self, populatedTaigaClient: TaigaClient):
        df = populatedTaigaClient.get(DATAFILE_ID)
        c = populatedTaigaClient.cache.conn.cursor()
        c.execute(
            "SELECT feather_path FROM datafiles WHERE full_taiga_id = ?", (DATAFILE_ID,)
        )

        feather_path = c.fetchone()[0]
        feather_content = open(feather_path, "rb").read()
        with open(feather_path, "wb+") as f:
            f.write(feather_content[: int(len(feather_content,) / 2)])

        df2 = populatedTaigaClient.get(DATAFILE_ID)
        df2.equals(df)


class TestDownloadToCache:
    def test_download_to_cache(self, taigaClient: TaigaClient):
        path = taigaClient.download_to_cache(DATAFILE_ID)
        assert path is not None
        df = pd.read_csv(path, index_col=0)
        assert df.loc["MDAMB453_BREAST", "breast"] == 1.0
        assert df.loc["MDAMB453_BREAST", "rhabdoid"] == 0.0

        datafile = taigaClient.cache._get_datafile_from_db(DATAFILE_ID, DATAFILE_ID)
        assert datafile.feather_path is None


class TestGetMetadata:
    def test_get_dataset_metadata(self, taigaClient: TaigaClient):
        dataset_metadata: DatasetMetadataDict = taigaClient.get_dataset_metadata(
            DATASET_PERMANAME
        )
        keys = dataset_metadata.keys()
        assert all(
            k in keys
            for k in [
                "can_edit",
                "can_view",
                "description",
                "folders",
                "id",
                "name",
                "permanames",
                "versions",
            ]
        )
        assert dataset_metadata["permanames"] == [DATASET_PERMANAME]

    def test_get_dataset_version_metadata(self, taigaClient: TaigaClient):
        dataset_metadata: DatasetMetadataDict = taigaClient.get_dataset_metadata(
            DATASET_PERMANAME
        )

        dataset_version_metadata: DatasetVersionMetadataDict = taigaClient.get_dataset_metadata(
            DATASET_PERMANAME, DATASET_VERSION
        )
        keys = dataset_version_metadata.keys()
        assert all(k in keys for k in ["dataset", "datasetVersion",])
        assert dataset_version_metadata["dataset"] == dataset_metadata


@pytest.mark.local
class TestCreateDataset:
    def test_create_dataset(self, monkeypatch, localTaigaClient: TaigaClient):
        monkeypatch.setattr("builtins.input", lambda _: "y")
        dataset_id = localTaigaClient.create_dataset(
            "taigapy test_create_dataset",
            dataset_description="Hello world",
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
        )

        assert dataset_id is not None
        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            dataset_id
        )
        assert dataset_metadata["name"] == "taigapy test_create_dataset"
        assert dataset_metadata["description"] == "Hello world"

    def test_input_validation(self, capsys, localTaigaClient: TaigaClient):
        assert localTaigaClient.create_dataset("taigapy test_input_validation") is None
        out, _ = capsys.readouterr()
        assert out.startswith("upload_files and add_taiga_ids cannot both be empty.")

    def test_invalid_file_fails(
        self, capsys, monkeypatch, localTaigaClient: TaigaClient
    ):
        monkeypatch.setattr("builtins.input", lambda _: "y")
        dataset_id = localTaigaClient.create_dataset(
            "taigapy test_invalid_file_fails",
            upload_files=[
                {
                    "path": "./tests/upload_files/empty_file.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
        )
        assert dataset_id is None
        out, _ = capsys.readouterr()
        assert "Error uploading empty_file: This file appears to be empty." in out

    def test_nonexistent_folder_fails(
        self, capsys, monkeypatch, localTaigaClient: TaigaClient
    ):
        dataset_id = localTaigaClient.create_dataset(
            "taigapy test_nonexistent_folder_fails",
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
            folder_id="nonexistent_folder",
        )
        assert dataset_id is None
        out, _ = capsys.readouterr()
        assert "No folder found with id nonexistent_folder." in out

    def test_duplicate_file_names_fails(
        self, capsys, monkeypatch, localTaigaClient: TaigaClient
    ):
        monkeypatch.setattr("builtins.input", lambda _: "y")
        dataset_id = localTaigaClient.create_dataset(
            "taigapy test_duplicate_file_names_fails",
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                },
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                },
            ],
        )
        assert dataset_id is None
        out, _ = capsys.readouterr()
        assert "Multiple files named matrix." in out


@pytest.mark.local
class TestUpdateDataset:
    def test_update_dataset(self, monkeypatch, localTaigaClient: TaigaClient):
        monkeypatch.setattr("builtins.input", lambda _: "y")
        dataset_id = localTaigaClient.create_dataset(
            "taigapy test_create_dataset",
            dataset_description="Hello world",
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
        )

        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            dataset_id
        )

        dataset_version_id = localTaigaClient.update_dataset(
            dataset_id,
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
            changes_description="Nothing really",
        )
        assert dataset_version_id is not None
        dataset_version_metadata: DatasetVersionMetadataDict = localTaigaClient.get_dataset_metadata(
            dataset_metadata["permanames"][0], 2
        )
        assert (
            dataset_version_metadata["datasetVersion"]["changes_description"]
            == "Nothing really"
        )
