import pytest
from .test_utils import taigaClient

@pytest.mark.parametrize(
    "taiga_id",
    [
        ("small-gecko-aff0.1"),  # not virtual, no file name
        ("small-gecko-virtual-dataset-4fe6.1"),  # virtual, no file name
        ("small-gecko-virtual-dataset-4fe6.1/gecko_score"),  # virtual, with file name
        ("small-gecko-aff0.1/gecko_score"),  # already canonical
    ],
)
def test_ask_taiga_for_canonical_taiga_id_single_file(taigaClient, taiga_id):
    expected_canonical = "small-gecko-aff0.1/gecko_score"
    assert taigaClient.get_canonical_id(taiga_id) == expected_canonical


def test_ask_taiga_for_canonical_id_multiple_file(taigaClient):
    """
    A bug previously caused leakage of the same canonical id across multiple file in the same dataset, if the dataset was not virtual
    We are testing that this does not happen
    :param app:
    :param taiga_id:
    :return:
    """
    virtual_dataset_root = "small-avana-virtual-dataset-86d8.1/"
    canonical_dataset_root = "small-avana-f2b9.2/"

    avana_file_name = "avana_score"
    duplicate_file_name = "avana_score_duplicate"

    # dataset is canonical (where the bug was first found)
    # asking for the first causes leakage to the second
    assert (
        taigaClient.get_canonical_id(canonical_dataset_root + avana_file_name)
        == canonical_dataset_root + avana_file_name
    )
    assert (
        taigaClient.get_canonical_id(canonical_dataset_root + duplicate_file_name)
        == canonical_dataset_root + duplicate_file_name
    )

    # dataset is virtual
    assert (
        taigaClient.get_canonical_id(virtual_dataset_root + avana_file_name)
        == canonical_dataset_root + avana_file_name
    )
    assert (
        taigaClient.get_canonical_id(virtual_dataset_root + duplicate_file_name)
        == canonical_dataset_root + duplicate_file_name
    )
