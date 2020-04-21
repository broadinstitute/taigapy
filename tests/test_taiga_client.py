import os
import pdb
import pytest
from typing import Dict
from unittest.mock import patch

import pandas as pd

from taigapy import TaigaClient
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

    @pytest.mark.parametrize(
        "get_inputs",
        [
            (
                dict(id=format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None))
            ),  # Datafile ID without file name
            (
                dict(
                    name=DATASET_PERMANAME, version=DATASET_VERSION, file=DATAFILE_NAME
                )
            ),  # Dataset name, version, datafile id entered separately
            (
                dict(name=DATASET_PERMANAME, version=DATASET_VERSION)
            ),  # Dataset name, version entered separately, no datafile name
            (
                dict(name=DATASET_PERMANAME, file=DATAFILE_NAME)
            ),  # Dataset name, datafile name entered separately, no dataset version
        ],
    )
    def test_get_input_formats(
        self, populatedTaigaClient: TaigaClient, get_inputs: Dict[str, str]
    ):
        with patch(
            "taigapy.taiga_api.TaigaApi.download_datafile"
        ) as mock_download_datafile:
            df = populatedTaigaClient.get(DATAFILE_ID)

            # Datafile ID without file name
            df2 = populatedTaigaClient.get(**get_inputs)
            assert not mock_download_datafile.called
            assert df2.equals(df)

    def test_get_virtual(self, taigaClient: TaigaClient):
        df = taigaClient.get("beat-aml-5d92.14/cPCA_cell_loadings")
        with patch(
            "taigapy.taiga_api.TaigaApi.download_datafile"
        ) as mock_download_datafile:
            df2 = taigaClient.get("beat-aml-5d92.17/cPCA_cell_loadings")
            assert not mock_download_datafile.called
            assert df2.equals(df)

    def test_get_raw(self, capsys, taigaClient: TaigaClient):
        assert taigaClient.get("test2-cdfa.17/foo") is None
        out, _ = capsys.readouterr()
        assert (
            "The file is a Raw one, please use instead `download_to_cache` with the same parameters"
            in out
        )

    def test_corrupted_feather(self, populatedTaigaClient: TaigaClient):
        df = populatedTaigaClient.get(DATAFILE_ID)
        c = populatedTaigaClient.cache.conn.cursor()
        c.execute(
            "SELECT feather_path FROM datafiles WHERE full_taiga_id = ?", (DATAFILE_ID,)
        )

        feather_path = c.fetchone()[0]
        feather_content = open(feather_path, "rb").read()
        with open(feather_path, "wb+") as f:
            f.write(feather_content[: int(len(feather_content) / 2)])

        df2 = populatedTaigaClient.get(DATAFILE_ID)
        df2.equals(df)

    def test_get_offline(self, capsys, populatedTaigaClient: TaigaClient):
        with patch.object(
            taigapy.taiga_api.TaigaApi, "is_connected", new=lambda cls: False
        ):
            assert populatedTaigaClient.get(DATAFILE_ID) is not None
            df = populatedTaigaClient.get(
                format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None)
            )
            assert df is None
        out, _ = capsys.readouterr()
        assert (
            "You are in offline mode, please be aware that you might be out of sync with the state of the dataset version (deprecation)"
            in out
        )
        assert "The datafile you requested was not in the cache" in out


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
        assert all(k in keys for k in ["dataset", "datasetVersion"])
        assert dataset_version_metadata["dataset"] == dataset_metadata


class TestGetCanonicalID:
    def test_get_canonical_id_full_taiga_id(self, taigaClient: TaigaClient):
        canonical_id = taigaClient.get_canonical_id(DATAFILE_ID)
        assert canonical_id == DATAFILE_ID

    def test_get_canonical_id_short_taiga_id(self, taigaClient: TaigaClient):
        canonical_id = taigaClient.get_canonical_id(
            format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None)
        )
        assert canonical_id == DATAFILE_ID

    def test_get_canonical_id_virtual_taiga_id(self, taigaClient: TaigaClient):
        canonical_id = taigaClient.get_canonical_id(
            "beat-aml-5d92.17/cPCA_cell_loadings"
        )
        assert canonical_id == "beat-aml-5d92.14/cPCA_cell_loadings"

    def test_get_canonical_id_populates_whole_dataset_version(
        self, taigaClient: TaigaClient
    ):
        canonical_id = taigaClient.get_canonical_id(
            "beat-aml-5d92.17/cPCA_cell_loadings"
        )
        assert canonical_id == "beat-aml-5d92.14/cPCA_cell_loadings"

        with patch(
            "taigapy.taiga_api.TaigaApi.get_datafile_metadata"
        ) as mock_get_datafile_metadata:
            canonical_id = taigaClient.get_canonical_id(
                "beat-aml-5d92.17/cPCA_gene_components"
            )
            assert canonical_id == "beat-aml-5d92.14/cPCA_gene_components"
            assert not mock_get_datafile_metadata.called


@pytest.mark.local
class TestCreateDataset:
    def test_create_dataset(self, localTaigaClient: TaigaClient):
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

    def test_invalid_file_fails(self, capsys, localTaigaClient: TaigaClient):
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

    def test_nonexistent_folder_fails(self, capsys, localTaigaClient: TaigaClient):
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

    def test_duplicate_file_names_fails(self, capsys, localTaigaClient: TaigaClient):
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


@pytest.fixture
def new_dataset(localTaigaClient: TaigaClient):
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
    return dataset_id


@pytest.mark.local
class TestUpdateDataset:
    def test_update_dataset(self, localTaigaClient: TaigaClient, new_dataset: str):
        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            new_dataset
        )

        dataset_version_id = localTaigaClient.update_dataset(
            new_dataset,
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

    def test_update_dataset_dataset_permaname(
        self, localTaigaClient: TaigaClient, new_dataset: str
    ):
        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            new_dataset
        )

        dataset_version_id = localTaigaClient.update_dataset(
            dataset_permaname=dataset_metadata["permanames"][0],
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

    def test_add_all_existing(self, localTaigaClient: TaigaClient, new_dataset: str):
        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            new_dataset
        )

        dataset_version_id = localTaigaClient.update_dataset(
            new_dataset,
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "name": "a new file name",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
            changes_description="Another (of the same) file",
            add_all_existing_files=True,
        )

        assert dataset_version_id is not None
        dataset_version_metadata: DatasetVersionMetadataDict = localTaigaClient.get_dataset_metadata(
            dataset_metadata["permanames"][0], 2
        )
        assert len(dataset_version_metadata["datasetVersion"]["datafiles"]) == 2

    def test_add_all_existing_same_file_name(
        self, localTaigaClient: TaigaClient, new_dataset: str
    ):
        dataset_metadata: DatasetMetadataDict = localTaigaClient.get_dataset_metadata(
            new_dataset
        )

        dataset_version_id = localTaigaClient.update_dataset(
            new_dataset,
            upload_files=[
                {
                    "path": "./tests/upload_files/matrix.csv",
                    "format": "NumericMatrixCSV",
                    "encoding": "utf-8",
                }
            ],
            changes_description="Another (of the same) file",
            add_all_existing_files=True,
        )

        assert dataset_version_id is not None
        dataset_version_metadata: DatasetVersionMetadataDict = localTaigaClient.get_dataset_metadata(
            dataset_metadata["permanames"][0], 2
        )
        assert len(dataset_version_metadata["datasetVersion"]["datafiles"]) == 1
