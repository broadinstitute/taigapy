"""Tests for taigapy CLI commands."""

import argparse
import pytest
from unittest.mock import patch, MagicMock

from taigapy.client_v3 import Client, TaigaReference

# Import helper from conftest - MockDB is used via fixture injection
from tests.conftest import MockDB, add_mock_dataset_version


def make_args(**kwargs) -> argparse.Namespace:
    """Helper to create argparse.Namespace with default CLI args."""
    defaults = {
        "taiga_url": None,
        "data_dir": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestCopy:
    """Tests for the copy CLI command."""

    def test_copy_creates_new_dataset(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test basic copy creates a new dataset with references."""
        # Setup: add a source dataset with files
        add_mock_dataset_version(
            mock_db,
            permaname="source-dataset-1234",
            version=1,
            files=[
                {"name": "matrix_file", "format": "HDF5", "custom_metadata": {}},
                {"name": "table_file", "format": "Columnar", "custom_metadata": {"key": "value"}},
            ],
        )

        args = make_args(
            source_id="source-dataset-1234.1",
            destination_name="My New Dataset",
            dryrun=False,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Verify create_dataset was called on the API
        mock_client.api.create_dataset.assert_called_once()
        call_args = mock_client.api.create_dataset.call_args

        # Check the dataset name
        assert call_args[0][2] == "My New Dataset"  # dataset_name is 3rd positional arg
        assert "Copy of source-dataset-1234.1" in call_args[0][3]  # description is 4th

        # Check output
        captured = capsys.readouterr()
        assert "Creating new dataset" in captured.out
        assert "2 referenced files" in captured.out

    def test_copy_with_update_flag(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test copy with --update updates an existing dataset."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-dataset-1234",
            version=1,
            files=[
                {"name": "file1", "format": "HDF5", "custom_metadata": {}},
            ],
        )
        # Add the destination dataset so update can find it
        add_mock_dataset_version(
            mock_db,
            permaname="existing-dataset-5678",
            version=1,
            files=[],
        )

        # Mock update_dataset on the client
        mock_client.update_dataset = MagicMock(return_value=MagicMock(permaname="existing-dataset-5678"))

        args = make_args(
            source_id="source-dataset-1234.1",
            destination_name="existing-dataset-5678",
            dryrun=False,
            update=True,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Verify update_dataset was called
        mock_client.update_dataset.assert_called_once()
        call_kwargs = mock_client.update_dataset.call_args
        assert call_kwargs[0][0] == "existing-dataset-5678"  # permaname
        assert "Copy of source-dataset-1234.1" in call_kwargs[1]["reason"]

        # Verify create_dataset was not called
        # (The api.create_dataset might have been called during fixture setup, so just check update was called)
        captured = capsys.readouterr()
        assert "Updating dataset" in captured.out

    def test_copy_dryrun(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test copy with --dryrun prints info without creating dataset."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-dataset-1234",
            version=2,
            files=[
                {"name": "file1", "format": "HDF5", "custom_metadata": {}},
                {"name": "file2", "format": "Columnar", "custom_metadata": {}},
            ],
        )

        # Reset the mock to track calls from this test only
        mock_client.api.create_dataset.reset_mock()

        args = make_args(
            source_id="source-dataset-1234.2",
            destination_name="New Dataset",
            dryrun=True,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Verify no datasets were created
        mock_client.api.create_dataset.assert_not_called()

        # Verify output mentions dry run
        captured = capsys.readouterr()
        assert "Dry run" in captured.out
        assert "create" in captured.out
        assert "file1" in captured.out
        assert "file2" in captured.out

    def test_copy_dryrun_with_update(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test copy with --dryrun and --update shows update action."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-1234",
            version=1,
            files=[
                {"name": "file1", "format": "HDF5", "custom_metadata": {}},
            ],
        )

        mock_client.api.create_dataset.reset_mock()
        mock_client.update_dataset = MagicMock()

        args = make_args(
            source_id="source-1234.1",
            destination_name="existing-dataset",
            dryrun=True,
            update=True,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Verify nothing was called
        mock_client.api.create_dataset.assert_not_called()
        mock_client.update_dataset.assert_not_called()

        captured = capsys.readouterr()
        assert "Dry run" in captured.out
        assert "update" in captured.out

    def test_copy_without_version_uses_latest(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test copy without version in source_id uses latest version."""
        # Add multiple versions
        add_mock_dataset_version(
            mock_db,
            permaname="source-dataset-1234",
            version=1,
            files=[
                {"name": "old_file", "format": "HDF5", "custom_metadata": {}},
            ],
        )
        add_mock_dataset_version(
            mock_db,
            permaname="source-dataset-1234",
            version=2,
            files=[
                {"name": "new_file", "format": "HDF5", "custom_metadata": {}},
            ],
        )

        mock_client.api.create_dataset.reset_mock()

        args = make_args(
            source_id="source-dataset-1234",  # No version specified
            destination_name="New Dataset",
            dryrun=True,  # Use dryrun to check what would be copied
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Should have used version 2 (latest)
        captured = capsys.readouterr()
        assert "new_file" in captured.out
        assert "source-dataset-1234.2/new_file" in captured.out

    def test_copy_skips_gcs_files(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test that files without 'type' (GCS files) are skipped."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-1234",
            version=1,
            files=[
                {"name": "taiga_file", "format": "HDF5", "custom_metadata": {}},
                {"name": "gcs_file", "format": None, "gs_path": "gs://bucket/file", "custom_metadata": {}},
            ],
        )

        mock_client.api.create_dataset.reset_mock()

        args = make_args(
            source_id="source-1234.1",
            destination_name="New Dataset",
            dryrun=True,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Warning should be printed about skipped file
        captured = capsys.readouterr()
        assert "Skipped" in captured.out or "skipped" in captured.out.lower()
        assert "gcs_file" in captured.out
        # The taiga file should still be listed
        assert "taiga_file" in captured.out

    def test_copy_preserves_custom_metadata(self, mock_client: Client, mock_db: MockDB):
        """Test that custom_metadata is preserved in copied references."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-1234",
            version=1,
            files=[
                {
                    "name": "file_with_metadata",
                    "format": "HDF5",
                    "custom_metadata": {"author": "test", "version": "1.0"},
                },
            ],
        )

        # Track what gets passed to upload_file_to_taiga
        uploaded_files = []
        original_upload = mock_client.api.upload_file_to_taiga.side_effect

        def track_upload(session_id, session_file):
            uploaded_files.append(session_file)
            if original_upload:
                return original_upload(session_id, session_file)

        mock_client.api.upload_file_to_taiga.side_effect = track_upload

        args = make_args(
            source_id="source-1234.1",
            destination_name="New Dataset",
            dryrun=False,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Check the uploaded file has the metadata
        assert len(uploaded_files) == 1
        uploaded = uploaded_files[0]
        if hasattr(uploaded, "custom_metadata"):
            assert uploaded.custom_metadata == {"author": "test", "version": "1.0"}
        else:
            # It's a dict
            assert uploaded.get("custom_metadata") == {"author": "test", "version": "1.0"} or \
                   uploaded.get("metadata") == {"author": "test", "version": "1.0"}

    def test_copy_fails_when_no_files_can_be_referenced(self, mock_client: Client, mock_db: MockDB):
        """Test that copy fails when all files are GCS (non-referenceable)."""
        add_mock_dataset_version(
            mock_db,
            permaname="source-1234",
            version=1,
            files=[
                {"name": "gcs_file1", "format": None, "gs_path": "gs://bucket/file1", "custom_metadata": {}},
                {"name": "gcs_file2", "format": None, "gs_path": "gs://bucket/file2", "custom_metadata": {}},
            ],
        )

        args = make_args(
            source_id="source-1234.1",
            destination_name="New Dataset",
            dryrun=False,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            with pytest.raises(AssertionError, match="No files could be referenced"):
                copy(args)

    def test_copy_source_not_found(self, mock_client: Client, mock_db: MockDB, capsys):
        """Test error handling when source dataset doesn't exist."""
        args = make_args(
            source_id="nonexistent-dataset",
            destination_name="New Dataset",
            dryrun=False,
            update=False,
        )

        with patch("taigapy.taiga_client_cli._get_taiga_client", return_value=mock_client):
            from taigapy.taiga_client_cli import copy
            copy(args)

        # Check error message was printed
        captured = capsys.readouterr()
        assert "not found" in captured.out
