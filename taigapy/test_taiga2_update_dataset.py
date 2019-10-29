"""Tests for creating and updating datasets.

The tests in this suite are written to run against a local instance of Taiga. See
https://github.com/broadinstitute/taiga for instructions on how to set that up.

Uploading files is not possible, so these tests are all on virtual files and datasets.
"""
import os
import pytest
from taigapy import TaigaClient


@pytest.fixture(scope="session")
def taigaClient(tmpdir_factory) -> TaigaClient:
    cache_dir = tmpdir_factory.mktemp("cache")
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)

    token_path = os.path.join(cache_dir, "token")

    with open(token_path, "w") as f:
        f.write("test-token")
    tc = TaigaClient(
        url="http://localhost:5000/taiga", cache_dir=cache_dir, token_path=token_path
    )
    return tc


def get_home_dir(taigaClient: TaigaClient) -> str:
    user = taigaClient.request_get(api_endpoint="/api/user")
    return user["home_folder_id"]


def get_folder_contents(taigaClient: TaigaClient, folder_id: str):
    folder = taigaClient.request_get(api_endpoint="/api/folder/{}".format(folder_id))
    return folder["entries"]


def get_origin_dataset(taigaClient: TaigaClient):
    home_folder_id = get_home_dir(taigaClient)
    home_folder_contents = get_folder_contents(taigaClient, home_folder_id)
    origin_dataset = next(
        entry
        for entry in home_folder_contents
        if entry["name"] == "origin" and entry["type"] == "dataset"
    )
    return taigaClient.request_get("/api/dataset/{}".format(origin_dataset["id"]))


def get_origin_file_id(taigaClient: TaigaClient, version: int = 1):
    origin_dataset = get_origin_dataset(taigaClient)
    origin_dataset_permaname = origin_dataset["permanames"][-1]
    if version == 1:
        origin_file_id = "{}.1/origin".format(origin_dataset_permaname)
    elif version == 2:
        origin_file_id = "{}.2/Datav1".format(origin_dataset_permaname)
    elif version == 3:
        origin_file_id = "{}.3/Datav1v2".format(origin_dataset_permaname)
    elif version == 4:
        origin_file_id = "{}.4/Datav1v2v3".format(origin_dataset_permaname)
    else:
        raise Exception("Unknown version: {}".format(version))
    return origin_file_id


def get_new_dataset(taigaClient: TaigaClient):
    home_folder_id = get_home_dir(taigaClient)
    home_folder_contents = get_folder_contents(taigaClient, home_folder_id)
    origin_dataset = next(
        entry
        for entry in home_folder_contents
        if entry["name"] == "new_dataset" and entry["type"] == "dataset"
    )
    return taigaClient.request_get("/api/dataset/{}".format(origin_dataset["id"]))


@pytest.mark.local
@pytest.mark.create_dataset
def test_create_dataset(taigaClient: TaigaClient):
    home_folder_id = get_home_dir(taigaClient)
    origin_v1_file_id = get_origin_file_id(taigaClient)

    dataset_id = taigaClient.create_dataset(
        dataset_name="new_dataset",
        dataset_description="new dataset description",
        add_taiga_ids=[("origin_v1_file_id", origin_v1_file_id)],
        folder_id=home_folder_id,
    )

    assert dataset_id is not None


@pytest.mark.local
def test_update_dataset_permaname_and_version(taigaClient: TaigaClient):
    new_dataset_permaname = get_new_dataset(taigaClient)["permanames"][-1]
    origin_v2_file_id = get_origin_file_id(taigaClient, version=2)

    new_dataset_version_id = taigaClient.update_dataset(
        dataset_permaname=new_dataset_permaname,
        dataset_version="1",
        add_taiga_ids=[("origin_v2_file_id", origin_v2_file_id)],
    )
    new_dataset_metadata = taigaClient.get_dataset_metadata(
        version_id=new_dataset_version_id
    )

    assert len(new_dataset_metadata["datafiles"]) == 1
    datafile = new_dataset_metadata["datafiles"][0]
    assert datafile["underlying_file_id"] == origin_v2_file_id


@pytest.mark.local
def test_update_dataset_permaname(taigaClient: TaigaClient):
    new_dataset_permaname = get_new_dataset(taigaClient)["permanames"][-1]
    origin_v3_file_id = get_origin_file_id(taigaClient, version=3)
    new_dataset_version_id = taigaClient.update_dataset(
        dataset_permaname=new_dataset_permaname,
        add_taiga_ids=[("origin_v3_file_id", origin_v3_file_id)],
    )
    new_dataset_metadata = taigaClient.get_dataset_metadata(
        version_id=new_dataset_version_id
    )

    assert len(new_dataset_metadata["datafiles"]) == 1
    datafile = new_dataset_metadata["datafiles"][0]
    assert datafile["underlying_file_id"] == origin_v3_file_id


@pytest.mark.local
def test_update_dataset_id(taigaClient: TaigaClient):
    new_dataset_id = get_new_dataset(taigaClient)["id"]
    origin_v4_file_id = get_origin_file_id(taigaClient, version=4)
    new_dataset_version_id = taigaClient.update_dataset(
        dataset_id=new_dataset_id,
        add_taiga_ids=[("origin_v4_file_id", origin_v4_file_id)],
    )
    new_dataset_metadata = taigaClient.get_dataset_metadata(
        version_id=new_dataset_version_id
    )

    assert len(new_dataset_metadata["datafiles"]) == 1
    datafile = new_dataset_metadata["datafiles"][0]
    assert datafile["underlying_file_id"] == origin_v4_file_id


@pytest.mark.local
def test_update_dataset_add_all_existing_files(taigaClient: TaigaClient):
    new_dataset_permaname = get_new_dataset(taigaClient)["permanames"][-1]
    origin_v1_file_id = get_origin_file_id(taigaClient, version=1)
    origin_v2_file_id = get_origin_file_id(taigaClient, version=2)

    new_dataset_version_id = taigaClient.update_dataset(
        dataset_permaname=new_dataset_permaname,
        dataset_version="1",
        add_taiga_ids=[("origin_v2_file_id", origin_v2_file_id)],
        add_all_existing_files=True,
    )
    new_dataset_metadata = taigaClient.get_dataset_metadata(
        version_id=new_dataset_version_id
    )

    assert len(new_dataset_metadata["datafiles"]) == 2
    assert set(
        datafile["underlying_file_id"] for datafile in new_dataset_metadata["datafiles"]
    ) == set([origin_v1_file_id, origin_v2_file_id])


@pytest.mark.local
def test_update_dataset_add_all_existing_files_ignore_same_name(
    taigaClient: TaigaClient
):
    new_dataset_permaname = get_new_dataset(taigaClient)["permanames"][-1]
    origin_v1_file_id = get_origin_file_id(taigaClient, version=1)
    origin_v2_file_id = get_origin_file_id(taigaClient, version=2)

    new_dataset_version_id = taigaClient.update_dataset(
        dataset_permaname=new_dataset_permaname,
        dataset_version="1",
        add_taiga_ids=[("origin_v1_file_id", origin_v2_file_id)],
        add_all_existing_files=True,
    )
    new_dataset_metadata = taigaClient.get_dataset_metadata(
        version_id=new_dataset_version_id
    )

    assert len(new_dataset_metadata["datafiles"]) == 1
    datafile = new_dataset_metadata["datafiles"][0]
    assert datafile["underlying_file_id"] == origin_v2_file_id
