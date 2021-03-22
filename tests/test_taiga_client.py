import os
import pdb
import pytest
from typing import Dict
from unittest.mock import patch

import pandas as pd

from taigapy import TaigaClient
import taigapy.taiga_api
import taigapy.utils
from taigapy.utils import format_datafile_id, get_latest_valid_version_from_metadata

from taigapy.custom_exceptions import TaigaTokenFileNotFound
from taigapy.types import DatasetMetadataDict, DatasetVersionMetadataDict


DATASET_PERMANAME = "depcon-binary-context-matrix"
DATASET_VERSION = 3
DATAFILE_NAME = "depcon_binary_context_matrix"
DATAFILE_ID = format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, DATAFILE_NAME)

PUBLIC_TAIGA_ID = "public-20q1-c3b6.14/Achilles_common_essentials"


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


@pytest.fixture
def figshareTaigaClient(tmpdir):
    cache_dir = str(tmpdir.join("cache"))

    tc = TaigaClient(
        cache_dir=cache_dir, figshare_map_file="./tests/figshare_map_file_sample.json"
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

        with pytest.raises(TaigaTokenFileNotFound):
            tc.get(DATAFILE_ID)


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

    def test_get_after_download_to_cache(self, taigaClient: TaigaClient):
        path = taigaClient.download_to_cache(DATAFILE_ID)
        assert path is not None

        df = taigaClient.get(DATAFILE_ID)
        assert df is not None

    def test_get_dataset_permaname_only(self, taigaClient: TaigaClient):
        df = taigaClient.get(name=DATASET_PERMANAME)
        dataset_metadata = taigaClient.get_dataset_metadata(DATASET_PERMANAME)
        dataset_version = get_latest_valid_version_from_metadata(dataset_metadata)
        c = taigaClient.cache.conn.cursor()

        c.execute("SELECT * FROM aliases WHERE alias = ?", (DATASET_PERMANAME,))
        assert c.fetchone() is None

        c.execute(
            "SELECT * FROM aliases WHERE alias = ?",
            (format_datafile_id(DATASET_PERMANAME, dataset_version, None),),
        )
        r = c.fetchone()

        assert r is not None

    @pytest.mark.parametrize(
        "get_inputs",
        [
            pytest.param(
                dict(id=format_datafile_id(DATASET_PERMANAME, DATASET_VERSION, None)),
                id="Datafile ID without file name",
            ),
            pytest.param(
                dict(
                    name=DATASET_PERMANAME, version=DATASET_VERSION, file=DATAFILE_NAME
                ),
                id="Dataset name, version, datafile id entered separately",
            ),
            pytest.param(
                dict(name=DATASET_PERMANAME, version=DATASET_VERSION),
                id="Dataset name, version entered separately, no datafile name",
            ),
            pytest.param(
                dict(name=DATASET_PERMANAME, file=DATAFILE_NAME),
                id="Dataset name, datafile name entered separately, no dataset version",
            ),
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

        dataset_version_metadata: DatasetVersionMetadataDict = (
            taigaClient.get_dataset_metadata(DATASET_PERMANAME, DATASET_VERSION)
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

    def test_get_canonical_id_short_virtual_taiga_id(self, taigaClient: TaigaClient):
        canonical_id = taigaClient.get_canonical_id(
            "small-gecko-virtual-dataset-4fe6.1"
        )
        assert canonical_id == "small-gecko-aff0.1/gecko_score"

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
        dataset_version_metadata: DatasetVersionMetadataDict = (
            localTaigaClient.get_dataset_metadata(dataset_metadata["permanames"][0], 2)
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
        dataset_version_metadata: DatasetVersionMetadataDict = (
            localTaigaClient.get_dataset_metadata(dataset_metadata["permanames"][0], 2)
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
        dataset_version_metadata: DatasetVersionMetadataDict = (
            localTaigaClient.get_dataset_metadata(dataset_metadata["permanames"][0], 2)
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
        dataset_version_metadata: DatasetVersionMetadataDict = (
            localTaigaClient.get_dataset_metadata(dataset_metadata["permanames"][0], 2)
        )
        assert len(dataset_version_metadata["datasetVersion"]["datafiles"]) == 1


class TestFigshare:
    def test_figshare_get(self, capsys, figshareTaigaClient: TaigaClient):
        df = figshareTaigaClient.get(PUBLIC_TAIGA_ID)
        assert df is not None
        assert df["gene"][0] == "AAAS (8086)"

        # Check that it caches file
        with patch(
            "taigapy.figshare.download_file_from_figshare"
        ) as mock_download_file_from_figshare:
            figshareTaigaClient.get(PUBLIC_TAIGA_ID)
            assert not mock_download_file_from_figshare.called

        # Check that it does not get the file from Taiga
        assert figshareTaigaClient.get(DATAFILE_ID) is None
        out, err = capsys.readouterr()
        assert out.startswith("{} is not in figshare_file_map".format(DATAFILE_ID))

    def test_figshare_download_to_cache(self, figshareTaigaClient: TaigaClient):
        path = figshareTaigaClient.download_to_cache(PUBLIC_TAIGA_ID)
        assert path is not None
        df = pd.read_csv(path)
        assert df["gene"][0] == "AAAS (8086)"
