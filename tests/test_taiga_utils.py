from typing import MutableSequence, Optional, List

import pytest

from taigapy.utils import untangle_dataset_id_with_version, modify_upload_files
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


@pytest.mark.parametrize(
    "upload_files,add_taiga_ids,dataset_version_metadata,add_all_existing_files,expected_upload_s3_datafiles,expected_upload_virtual_datafiles",
    [
        pytest.param(
            upload_files,
            add_taiga_ids,
            dataset_version_metadata,
            add_all_existing_files,
            expected_upload_s3_datafiles,
            expected_upload_virtual_datafiles,
            id="",
        )
    ],
)
def test_modify_upload_files(
    upload_files: MutableSequence[UploadS3DataFileDict],
    add_taiga_ids: MutableSequence[UploadVirtualDataFileDict],
    dataset_version_metadata: Optional[DatasetVersionMetadataDict],
    add_all_existing_files: bool,
    expected_upload_s3_datafiles: List[UploadS3DataFile],
    expected_upload_virtual_datafiles: List[UploadVirtualDataFile],
):
    pass
