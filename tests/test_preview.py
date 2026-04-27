import numpy as np
import pandas as pd

from taigapy.client_v3 import (
    Client,
    LocalFormat,
    UploadedFile,
    _generate_preview_for_file,
    _json_safe,
)
from taigapy.consts import PREVIEW_MAX_ROWS, PREVIEW_MAX_COLUMNS
from taigapy.format_utils import write_hdf5, write_parquet


# ---------------------------------------------------------------------------
# Test _json_safe
# ---------------------------------------------------------------------------

class TestJsonSafe:
    def test_nan(self):
        assert _json_safe(float("nan")) is None

    def test_inf(self):
        assert _json_safe(float("inf")) is None
        assert _json_safe(float("-inf")) is None

    def test_none(self):
        assert _json_safe(None) is None

    def test_numpy_int(self):
        result = _json_safe(np.int64(42))
        assert result == 42
        assert type(result) is int

    def test_numpy_float(self):
        result = _json_safe(np.float32(3.14))
        assert isinstance(result, float)
        assert abs(result - 3.14) < 0.001

    def test_numpy_nan(self):
        assert _json_safe(np.float64("nan")) is None

    def test_numpy_bool(self):
        result = _json_safe(np.bool_(True))
        assert result is True
        assert type(result) is bool

    def test_regular_values_pass_through(self):
        assert _json_safe("hello") == "hello"
        assert _json_safe(42) == 42
        assert _json_safe(3.14) == 3.14


# ---------------------------------------------------------------------------
# Test _generate_preview_for_file — one test per format
# ---------------------------------------------------------------------------

class TestGeneratePreviewCSVTable:
    def test_basic(self, tmp_path):
        df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
        path = str(tmp_path / "table.csv")
        df.to_csv(path, index=False)

        preview = _generate_preview_for_file(path, LocalFormat.CSV_TABLE)

        assert preview["num_rows"] == 3
        assert preview["num_columns"] == 2
        tlp = preview["top_left_preview"]
        assert tlp["column_names"] == ["col_a", "col_b"]
        assert tlp["row_names"] is None
        assert len(tlp["data"]) == 3
        assert tlp["data"][0] == [1, "x"]


class TestGeneratePreviewCSVMatrix:
    def test_basic(self, tmp_path):
        df = pd.DataFrame(
            {"gene_a": [0.1, 0.2], "gene_b": [0.3, 0.4]},
            index=["cell_1", "cell_2"],
        )
        path = str(tmp_path / "matrix.csv")
        df.to_csv(path)

        preview = _generate_preview_for_file(path, LocalFormat.CSV_MATRIX)

        assert preview["num_rows"] == 2
        assert preview["num_columns"] == 2
        tlp = preview["top_left_preview"]
        assert tlp["column_names"] == ["gene_a", "gene_b"]
        assert tlp["row_names"] == ["cell_1", "cell_2"]
        assert tlp["data"][0] == [0.1, 0.3]


class TestGeneratePreviewHDF5:
    def test_basic(self, tmp_path):
        df = pd.DataFrame(
            {"g1": [1.0, 2.0, 3.0], "g2": [4.0, 5.0, 6.0]},
            index=["r1", "r2", "r3"],
        )
        path = str(tmp_path / "matrix.hdf5")
        write_hdf5(df, path)

        preview = _generate_preview_for_file(path, LocalFormat.HDF5_MATRIX)

        assert preview["num_rows"] == 3
        assert preview["num_columns"] == 2
        tlp = preview["top_left_preview"]
        assert tlp["column_names"] == ["g1", "g2"]
        assert tlp["row_names"] == ["r1", "r2", "r3"]
        assert tlp["data"][0] == [1.0, 4.0]


class TestGeneratePreviewParquet:
    def test_basic(self, tmp_path):
        df = pd.DataFrame({"x": [10, 20], "y": [30, 40]})
        path = str(tmp_path / "table.parquet")
        write_parquet(df, path)

        preview = _generate_preview_for_file(path, LocalFormat.PARQUET_TABLE)

        assert preview["num_rows"] == 2
        assert preview["num_columns"] == 2
        tlp = preview["top_left_preview"]
        assert tlp["column_names"] == ["x", "y"]
        assert tlp["row_names"] is None
        assert tlp["data"] == [[10, 30], [20, 40]]


class TestGeneratePreviewRaw:
    def test_returns_none(self, tmp_path):
        path = str(tmp_path / "blob.bin")
        with open(path, "wb") as f:
            f.write(b"\x00\x01\x02")
        assert _generate_preview_for_file(path, LocalFormat.RAW) is None


# ---------------------------------------------------------------------------
# Truncation: preview respects PREVIEW_MAX_ROWS / PREVIEW_MAX_COLUMNS
# ---------------------------------------------------------------------------

class TestPreviewTruncation:
    def test_csv_table_truncates(self, tmp_path):
        n_rows = PREVIEW_MAX_ROWS + 20
        n_cols = PREVIEW_MAX_COLUMNS + 10
        df = pd.DataFrame(
            np.random.rand(n_rows, n_cols),
            columns=[f"c{i}" for i in range(n_cols)],
        )
        path = str(tmp_path / "big.csv")
        df.to_csv(path, index=False)

        preview = _generate_preview_for_file(path, LocalFormat.CSV_TABLE)

        assert preview["num_rows"] == n_rows
        assert preview["num_columns"] == n_cols
        tlp = preview["top_left_preview"]
        assert len(tlp["column_names"]) == PREVIEW_MAX_COLUMNS
        assert len(tlp["data"]) == PREVIEW_MAX_ROWS
        assert len(tlp["data"][0]) == PREVIEW_MAX_COLUMNS

    def test_hdf5_truncates(self, tmp_path):
        n_rows = PREVIEW_MAX_ROWS + 10
        n_cols = PREVIEW_MAX_COLUMNS + 5
        df = pd.DataFrame(
            np.random.rand(n_rows, n_cols),
            columns=[f"c{i}" for i in range(n_cols)],
            index=[f"r{i}" for i in range(n_rows)],
        )
        path = str(tmp_path / "big.hdf5")
        write_hdf5(df, path)

        preview = _generate_preview_for_file(path, LocalFormat.HDF5_MATRIX)

        assert preview["num_rows"] == n_rows
        assert preview["num_columns"] == n_cols
        tlp = preview["top_left_preview"]
        assert len(tlp["column_names"]) == PREVIEW_MAX_COLUMNS
        assert len(tlp["row_names"]) == PREVIEW_MAX_ROWS
        assert len(tlp["data"]) == PREVIEW_MAX_ROWS
        assert len(tlp["data"][0]) == PREVIEW_MAX_COLUMNS


# ---------------------------------------------------------------------------
# NaN handling in generated previews
# ---------------------------------------------------------------------------

class TestPreviewNaNHandling:
    def test_csv_with_nan(self, tmp_path):
        df = pd.DataFrame({"a": [1.0, float("nan"), 3.0], "b": [float("nan"), 5.0, 6.0]})
        path = str(tmp_path / "nan.csv")
        df.to_csv(path, index=False)

        preview = _generate_preview_for_file(path, LocalFormat.CSV_TABLE)
        data = preview["top_left_preview"]["data"]

        assert data[0] == [1.0, None]
        assert data[1] == [None, 5.0]


# ---------------------------------------------------------------------------
# Test Integration: create_dataset triggers preview upload
# ---------------------------------------------------------------------------

class TestUploadPreviewIntegration:
    def test_create_dataset_posts_preview(self, mock_client: Client, tmpdir, s3_mock_client):
        sample_file = tmpdir.join("table.csv")
        df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
        df.to_csv(str(sample_file), index=False)

        mock_client.create_dataset(
            "test",
            "desc",
            [
                UploadedFile(
                    name="my_table",
                    local_path=str(sample_file),
                    format=LocalFormat.CSV_TABLE,
                    custom_metadata={},
                )
            ],
        )

        mock_client.api.post_datafile_preview.assert_called_once()
        call_args = mock_client.api.post_datafile_preview.call_args
        preview_data = call_args[0][1]
        assert preview_data["num_rows"] == 2
        assert preview_data["num_columns"] == 2
        assert preview_data["top_left_preview"]["column_names"] == ["x", "y"]

    def test_preview_failure_does_not_block_upload(self, mock_client: Client, tmpdir, s3_mock_client):
        """If preview POST fails, create_dataset still returns successfully."""
        mock_client.api.post_datafile_preview.side_effect = Exception("server error")

        sample_file = tmpdir.join("table.csv")
        df = pd.DataFrame({"x": [1, 2]})
        df.to_csv(str(sample_file), index=False)

        version = mock_client.create_dataset(
            "test",
            "desc",
            [
                UploadedFile(
                    name="my_table",
                    local_path=str(sample_file),
                    format=LocalFormat.CSV_TABLE,
                    custom_metadata={},
                )
            ],
        )

        assert len(version.files) == 1
        mock_client.api.post_datafile_preview.assert_called_once()
