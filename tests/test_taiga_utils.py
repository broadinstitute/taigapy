import copy
import py
from typing import Dict, Sequence, Optional, List

import numpy as np
import pandas as pd
import pytest

from taigapy.utils import (
    untangle_dataset_id_with_version,
    transform_upload_args_to_upload_list,
)
from taigapy.types import (
    DatasetVersionMetadataDict,
    UploadS3DataFile,
    UploadS3DataFileDict,
    UploadVirtualDataFile,
    UploadVirtualDataFileDict,
)


@pytest.mark.parametrize(
    "test_input,expected,expect_error",
    [
        ("foo.10/bar", ("foo", "10", "bar"), False),
        ("foo.10", ("foo", "10", None), False),
        ("foo-bar_baz.10", ("foo-bar_baz", "10", None), False),
        ("foo.bar", ("foo-bar_baz", "10", None), True),
    ],
)
def test_untangle_dataset_id_with_version(
    test_input: str, expected: str, expect_error: bool
):
    if not expect_error:
        assert untangle_dataset_id_with_version(test_input) == expected
    else:
        with pytest.raises(ValueError):
            untangle_dataset_id_with_version(test_input)


upload_files: List[UploadS3DataFileDict] = [
    {"path": "matrix.csv", "name": "Matrix", "format": "NumericMatrixCSV"},
    {"path": "matrix_no_name.csv", "format": "NumericMatrixCSV"},
    {"path": "table.csv", "name": "Table", "format": "TableCSV"},
    {"path": "table_no_name.csv", "format": "TableCSV"},
    {"path": "raw.txt", "name": "matching_datafile", "format": "Raw"},
    {"path": "raw_no_name.txt", "format": "Raw"},
]
add_taiga_ids: List[UploadVirtualDataFileDict] = [
    {"taiga_id": "foo.1/bar", "name": "Bar"},
    {"taiga_id": "foo.1/no_name"},
]
dataset_version_metadata: DatasetVersionMetadataDict = {
    "dataset": {
        "can_edit": True,
        "can_view": True,
        "description": "dataset description",
        "folders": [{"id": "folder-123", "name": "Folder"}],
        "id": "dataset-id",
        "name": "Dataset",
        "permanames": ["dataset-0001"],
        "versions": [
            {
                "id": "version-id-1",
                "name": "1",
                "state": "approved",
            }
        ],
    },
    "datasetVersion": {
        "can_edit": True,
        "can_view": True,
        "changes_description": "changes",
        "creation_date": "",
        "creator": {},
        "datafiles": [
            {
                "datafile_type": "s3",
                "id": "datafile-1",
                "name": "matching_datafile",
                "short_summary": "",
                "type": "Raw",
                "original_file_md5": "a6087079aec78a575b7057d73bac3ec7",
                "original_file_sha256": "91e3022709412414c5018b22ff545b0dda2a839c399fc3ffacb8e076568d4776",
            },
            {
                "datafile_type": "s3",
                "id": "datafile-2",
                "name": "non_matching_datafile",
                "short_summary": "",
                "type": "Raw",
                "original_file_md5": "",
                "original_file_sha256": "",
            },
        ],
        "dataset_id": "dataset-id",
        "description": "dataset version description",
        "id": "version-id-1",
        "name": "1",
        "reason_state": "",
        "state": "approved",
        "version": "1",
    },
}

expected_upload_s3_datafiles_api_params: List[Dict] = [
    {
        "filename": "Matrix",
        "filetype": "s3",
        "s3Upload": {
            "format": "NumericMatrixCSV",
            "bucket": None,
            "key": None,
            "encoding": "utf-8",
        },
    },
    {
        "filename": "matrix_no_name",
        "filetype": "s3",
        "s3Upload": {
            "format": "NumericMatrixCSV",
            "bucket": None,
            "key": None,
            "encoding": "utf-8",
        },
    },
    {
        "filename": "Table",
        "filetype": "s3",
        "s3Upload": {
            "format": "TableCSV",
            "bucket": None,
            "key": None,
            "encoding": "utf-8",
        },
    },
    {
        "filename": "table_no_name",
        "filetype": "s3",
        "s3Upload": {
            "format": "TableCSV",
            "bucket": None,
            "key": None,
            "encoding": "utf-8",
        },
    },
    {
        "filename": "matching_datafile",
        "filetype": "s3",
        "s3Upload": {"format": "Raw", "bucket": None, "key": None, "encoding": "utf-8"},
    },
    {
        "filename": "raw_no_name",
        "filetype": "s3",
        "s3Upload": {"format": "Raw", "bucket": None, "key": None, "encoding": "utf-8"},
    },
]
expected_upload_virtual_datafiles_api_params: List[Dict] = [
    {"filename": "Bar", "filetype": "virtual", "existingTaigaId": "foo.1/bar"},
    {
        "filename": "no_name",
        "filetype": "virtual",
        "existingTaigaId": "foo.1/no_name",
    },
]

expected_matching_datafiles_api_params: List[Dict] = [
    {
        "filename": "matching_datafile",
        "filetype": "virtual",
        "existingTaigaId": "dataset-0001.1/matching_datafile",
    }
]

matrix_df = pd.DataFrame({"a": [2.1, np.nan], "b": [1.1, 1.4]}, index=["c", "d"])
table_df = pd.DataFrame({"a": [2.1, np.nan], "b": ["one", "two"]})


@pytest.mark.parametrize(
    "upload_files,add_taiga_ids,dataset_version_metadata,expected_upload_s3_datafiles_api_params,expected_upload_virtual_datafiles_api_params,expected_matching_datafiles_api_params",
    [
        pytest.param(
            copy.deepcopy(upload_files),
            [],
            None,
            expected_upload_s3_datafiles_api_params,
            [],
            [],
            id="create dataset, only upload files",
        ),
        pytest.param(
            [],
            copy.deepcopy(add_taiga_ids),
            None,
            [],
            expected_upload_virtual_datafiles_api_params,
            [],
            id="create dataset, only virtual files",
        ),
        pytest.param(
            copy.deepcopy(upload_files),
            copy.deepcopy(add_taiga_ids),
            None,
            expected_upload_s3_datafiles_api_params,
            expected_upload_virtual_datafiles_api_params,
            [],
            id="create dataset, both upload and virtual files",
        ),
        pytest.param(
            copy.deepcopy(upload_files),
            [],
            dataset_version_metadata,
            [
                f
                for f in expected_upload_s3_datafiles_api_params
                if f["filename"] != "matching_datafile"
            ],
            [],
            expected_matching_datafiles_api_params,
            id="update dataset, only upload files, skip existsing files",
        ),
        pytest.param(
            copy.deepcopy(upload_files),
            [],
            dataset_version_metadata,
            [
                f
                for f in expected_upload_s3_datafiles_api_params
                if f["filename"] != "matching_datafile"
            ],
            [],
            expected_matching_datafiles_api_params,
            id="update dataset, only upload files, add existing files",
        ),
        pytest.param(
            [],
            copy.deepcopy(add_taiga_ids),
            dataset_version_metadata,
            [],
            expected_upload_virtual_datafiles_api_params,
            [],
            id="update dataset, only virtual files",
        ),
        pytest.param(
            copy.deepcopy(upload_files),
            copy.deepcopy(add_taiga_ids),
            dataset_version_metadata,
            [
                f
                for f in expected_upload_s3_datafiles_api_params
                if f["filename"] != "matching_datafile"
            ],
            expected_upload_virtual_datafiles_api_params,
            expected_matching_datafiles_api_params,
            id="update dataset, both upload and virtual files",
        ),
    ],
)
def test_modify_upload_files(
    upload_files: Sequence[UploadS3DataFileDict],
    add_taiga_ids: Sequence[UploadVirtualDataFileDict],
    dataset_version_metadata: Optional[DatasetVersionMetadataDict],
    expected_upload_s3_datafiles_api_params: List[Dict],
    expected_upload_virtual_datafiles_api_params: List[Dict],
    expected_matching_datafiles_api_params: List[Dict],
    tmpdir,
):
    for upload_file_dict in upload_files:
        p = tmpdir.join(upload_file_dict["path"])
        upload_file_dict["path"] = p
        if upload_file_dict["format"] == "NumericMatrixCSV":
            matrix_df.to_csv(p)
        elif upload_file_dict["format"] == "TableCSV":
            table_df.to_csv(p, index=False)
        elif upload_file_dict.get("name") == "matching_datafile":
            matrix_df.to_json(p)
        else:
            table_df.to_json(p)

    upload_datafiles = transform_upload_args_to_upload_list(
        upload_files,
        add_taiga_ids,
        [],
        dataset_version_metadata=dataset_version_metadata,
    )

    upload_s3_datafiles = []

    upload_virtual_datafiles = []
    for upload_datafile in upload_datafiles:
        if isinstance(upload_datafile, UploadS3DataFile):
            upload_s3_datafiles.append(upload_datafile)
        elif isinstance(upload_datafile, UploadVirtualDataFile):
            upload_virtual_datafiles.append(upload_datafile)
        else:
            raise NotImplementedError()

    assert len(upload_s3_datafiles) == len(expected_upload_s3_datafiles_api_params)
    assert all(
        actual.to_api_param() == expected
        for (actual, expected) in zip(
            upload_s3_datafiles, expected_upload_s3_datafiles_api_params
        )
    )

    assert len(upload_virtual_datafiles) == len(
        expected_upload_virtual_datafiles_api_params
        + expected_matching_datafiles_api_params
    )
    assert all(
        actual.to_api_param() == expected
        for (actual, expected) in zip(
            upload_virtual_datafiles,
            (
                expected_upload_virtual_datafiles_api_params
                + expected_matching_datafiles_api_params
            ),
        )
    )
