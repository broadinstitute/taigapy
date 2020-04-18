import os
import shutil
import sqlite3
import pandas as pd

from collections import namedtuple
from typing import Mapping, Optional, Tuple

from taigapy.custom_exceptions import TaigaCacheFileCorrupted, TaigaRawTypeException
from taigapy.types import DataFileType, DataFileFormat

# Also needs to handle case where a virtual dataset contains only one file... ? Should that be an alias?

# TODO: Redo this... Remove underlying_data_file and just lump that in with alias. I don't think cache needs
#       to know whether something is an alias or underlying file, as long as it returns the right df
DataFile = namedtuple(
    "DataFile", ["full_taiga_id", "raw_path", "feather_path", "datafile_format",],
)

GET_QUERY = """
    SELECT datafiles.full_taiga_id, raw_path, feather_path, datafile_format
    FROM datafiles
    LEFT JOIN aliases
    ON
        datafiles.full_taiga_id = aliases.full_taiga_id
    WHERE
        alias = ? OR
        datafiles.full_taiga_id = ?
    """


def _write_csv_to_feather(
    csv_path: str,
    feather_path: str,
    datafile_format: DataFileFormat,
    column_types: Optional[Mapping[str, str]] = None,
    encoding: Optional[str] = None,
) -> str:
    if datafile_format == DataFileFormat.HDF5:
        # https://github.com/pandas-dev/pandas/issues/25067
        df = pd.read_csv(csv_path, index_col=0, encoding=encoding)
        df = df.astype(float)

        # Feather does not support indexes
        df.reset_index().to_feather(feather_path)
    else:
        df = pd.read_csv(csv_path, dtype=column_types, encoding=encoding)
        df.to_feather(feather_path)

    return feather_path


def _read_feather_to_df(path: str, datafile_format: DataFileFormat) -> pd.DataFrame:
    """Reads and returns a Pandas DataFrame from a Feather file at `path`.

    If `datafile_format` is "HDF5", we convert the first column to an index.
    """
    df = pd.read_feather(path)
    if datafile_format == DataFileFormat.HDF5:
        df.set_index(df.columns[0], inplace=True)
        df.index.name = None
    return df


class TaigaCache:
    def __init__(self, cache_dir=str, cache_file_path=str):
        self.cache_dir = cache_dir
        self.cache_file_path = cache_file_path

        cache_exists = os.path.exists(self.cache_file_path)

        self.conn = sqlite3.connect(self.cache_file_path)

        if not cache_exists:
            self._create_db()

    def _create_db(self):
        c = self.conn.cursor()

        # Create table
        c.execute(
            """
            CREATE TABLE datafiles(
                full_taiga_id TEXT NOT NULL PRIMARY KEY,
                raw_path TEXT,
                feather_path TEXT,
                datafile_format TEXT NOT NULL
            )
            """
        )

        c.execute(
            """
            CREATE TABLE aliases(
                alias TEXT NOT NULL PRIMARY KEY,
                full_taiga_id TEXT NOT NULL,
                FOREIGN KEY (full_taiga_id)
                    REFERENCES datafiles (full_taiga_id)
            )
            """
        )

        c.close()

        # Save (commit) the changes
        self.conn.commit()

    def _get_path_and_make_directories(
        self, full_taiga_id: str, extension: str,
    ) -> str:
        assert not extension.startswith(".")
        rel_file_path_without_extension = full_taiga_id.replace(".", "/")
        file_path = os.path.join(
            self.cache_dir, "{}.{}".format(rel_file_path_without_extension, extension),
        )
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        return file_path

    def _get_datafile_from_db(
        self, queried_taiga_id: str, full_taiga_id: str
    ) -> DataFile:
        c = self.conn.cursor()
        c.execute(
            GET_QUERY, (queried_taiga_id, full_taiga_id),
        )

        r = c.fetchone()
        c.close()

        if r is None:
            return None

        return DataFile(*r)

    def _add_alias(self, queried_taiga_id: str, full_taiga_id: str):
        if queried_taiga_id == full_taiga_id:
            return

        c = self.conn.cursor()
        c.execute(
            """
            SELECT
                full_taiga_id
            FROM
                aliases
            where
                alias = ?
            """,
            (queried_taiga_id,),
        )
        if c.fetchone() is None:
            c.execute(
                """
                INSERT INTO aliases
                VALUES (?, ?)
                """,
                (queried_taiga_id, full_taiga_id),
            )
        c.close()
        self.conn.commit()

    def get_entry(
        self, queried_taiga_id: str, full_taiga_id: str
    ) -> Optional[pd.DataFrame]:
        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)
        if datafile is None:
            return None

        if datafile.datafile_format == DataFileFormat.Raw:
            raise TaigaRawTypeException()

        self._add_alias(queried_taiga_id, full_taiga_id)

        if datafile.feather_path is None:
            return None

        try:
            return _read_feather_to_df(
                datafile.feather_path, DataFileFormat(datafile.datafile_format)
            )
        except Exception as e:
            self.remove_from_cache(queried_taiga_id, full_taiga_id)
            raise TaigaCacheFileCorrupted()

    def get_raw_path(self, queried_taiga_id: str, full_taiga_id: str) -> Optional[str]:
        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)
        if datafile is None:
            return None

        raw_path = datafile.raw_path
        if not os.path.exists(raw_path):
            if datafile.feather_path is None:
                self.remove_from_cache(queried_taiga_id, full_taiga_id)
            return None

        return raw_path

    def get_full_taiga_id(self, queried_taiga_id: str) -> Optional[str]:
        datafile = self._get_datafile_from_db(queried_taiga_id, queried_taiga_id)
        if datafile is None:
            return None

        return datafile.full_taiga_id

    def add_entry(
        self,
        raw_path: Optional[str],
        queried_taiga_id: str,
        full_taiga_id: str,
        datafile_format: DataFileFormat,
        column_types: Optional[Mapping[str, str]],
        encoding: Optional[str],
    ):
        """
        TODO:
        if already exists:
            return
        if datafile type is s3:
            - generate path for feather
            - create subdirectories if needed
            - convert df to feather
            - add to cache
        else if datafile type is virtual:
            (TaigaClient flow should have already added the real file?)
            - 
        """

        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)

        c = self.conn.cursor()
        if datafile is None:
            if datafile_format == DataFileFormat.Raw:
                raw_cache_path = self._get_path_and_make_directories(
                    full_taiga_id, "txt"
                )
                shutil.copy(raw_path, raw_cache_path)
                c.execute(
                    """
                    INSERT INTO datafiles
                    VALUES (?, ?, ?, ?)
                    """,
                    (full_taiga_id, raw_cache_path, None, datafile_format.value),
                )
            else:
                raw_cache_path = self._get_path_and_make_directories(
                    full_taiga_id, "csv",
                )
                feather_path = self._get_path_and_make_directories(
                    full_taiga_id, "feather"
                )
                shutil.copy(raw_path, raw_cache_path)
                _write_csv_to_feather(
                    raw_cache_path,
                    feather_path,
                    datafile_format,
                    column_types,
                    encoding,
                )
                c.execute(
                    """
                    INSERT INTO datafiles
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        full_taiga_id,
                        raw_cache_path,
                        feather_path,
                        datafile_format.value,
                    ),
                )
        else:
            if datafile.raw_path is None:
                self.add_raw_entry(
                    raw_path, queried_taiga_id, full_taiga_id, datafile_format
                )
                datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)
            if (
                datafile.feather_path is None
                and datafile.datafile_format != DataFileFormat.Raw
            ):
                if datafile_format == DataFileFormat.Columnar:
                    assert column_types is not None

                feather_path = self._get_path_and_make_directories(
                    full_taiga_id, "feather"
                )
                try:
                    _write_csv_to_feather(
                        datafile.raw_path,
                        feather_path,
                        datafile_format,
                        column_types,
                        encoding,
                    )
                except FileNotFoundError:
                    shutil.copy(raw_path, datafile.raw_path)
                    _write_csv_to_feather(
                        datafile.raw_path,
                        feather_path,
                        datafile_format,
                        column_types,
                        encoding,
                    )
                c.execute(
                    """
                    UPDATE datafiles
                    SET feather_path = ?
                    WHERE
                        full_taiga_id = ?
                    """,
                    (feather_path, full_taiga_id),
                )

        self._add_alias(queried_taiga_id, full_taiga_id)
        c.close()
        self.conn.commit()

    def add_raw_entry(
        self,
        raw_path: str,
        queried_taiga_id: str,
        full_taiga_id: str,
        datafile_format: DataFileFormat,
    ):
        "Copies the file at `raw_path` into the cache directory, and stores an entry in the cache."
        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)

        cache_file_extension = "txt" if datafile_format == DataFileFormat.Raw else "csv"
        cache_file_path = self._get_path_and_make_directories(
            full_taiga_id, cache_file_extension
        )

        shutil.copy(raw_path, cache_file_path)
        c = self.conn.cursor()
        if datafile is None:
            c.execute(
                """
                INSERT INTO datafiles (full_taiga_id, raw_path, datafile_format)
                VALUES (?, ?, ?)
                """,
                (full_taiga_id, cache_file_path, datafile_format.value),
            )
        else:
            c.execute(
                """
                UPDATE datafiles
                SET raw_path = ?
                WHERE full_taiga_id = ?
                """,
                (cache_file_path, full_taiga_id),
            )
        self._add_alias(queried_taiga_id, full_taiga_id)
        c.close()
        self.conn.commit()

    def add_full_id(
        self, queried_taiga_id: str, full_taiga_id: str, datafile_format: DataFileFormat
    ):
        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)
        if datafile is None:
            c = self.conn.cursor()
            c.execute(
                """
                INSERT INTO datafiles (full_taiga_id, datafile_format)
                VALUES (?, ?)
                """,
                (full_taiga_id, datafile_format.value),
            )
            c.close()
            self.conn.commit()

        self._add_alias(queried_taiga_id, full_taiga_id)

    def remove_from_cache(self, queried_taiga_id: str, full_taiga_id: str):
        datafile = self._get_datafile_from_db(queried_taiga_id, full_taiga_id)

        c = self.conn.cursor()
        c.execute(
            """
            DELETE FROM datafiles
            WHERE full_taiga_id = ?
            """,
            (datafile.full_taiga_id,),
        )

        c.execute(
            """
            DELETE FROM datafiles
            WHERE full_taiga_id = ?
            """,
            (datafile.full_taiga_id,),
        )

        c.close()
        self.conn.commit()

        for p in [datafile.feather_path, datafile.raw_path]:
            if os.path.exists(p):
                os.remove(p)

    def remove_all_from_cache(self, prefix: str):
        if not prefix.endswith("/"):
            raise ValueError("Prefix must end in /")

        c = self.conn.cursor()

        c.execute(
            """
            SELECT alias FROM aliases
            WHERE
                full_taiga_id LIKE '?%'
            """,
            (prefix,),
        )

        aliases_to_delete = [r[0] for r in c.fetchall()]

        c.execute(
            """
            DELETE FROM aliases
            WHERE alias IN ({})
            """.format(
                ",".join("?" * len(aliases_to_delete))
            ),
            tuple(aliases_to_delete),
        )

        c.execute(
            """
            SELECT full_taiga_id, raw_path, feather_path, datafile_format FROM datafiles
            WHERE
                full_taiga_id LIKE '?%' or
                underlyfing_file_id LIKE '?%'
            """,
            (prefix, prefix),
        )

        datafiles_to_delete = [DataFile(*r) for r in c.fetchall()]
        c.execute(
            """
            DELETE FROM datafiles
            WHERE full_taiga_id IN ({})
            """.format(
                ",".join("?" * len(datafiles_to_delete))
            ),
            tuple([datafile.full_taiga_id for datafile in datafiles_to_delete]),
        )
        c.close()
        self.conn.commit()

        for datafile in datafiles_to_delete:
            if datafile.raw_path is not None and os.path.exists(datafile.raw_path):
                os.remove(datafile.raw_path)

            if datafile.feather_path is not None and os.path.exists(
                datafile.feather_path
            ):
                os.remove(datafile.feather_path)
