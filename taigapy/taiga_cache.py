import os
import sqlite3

from collections import namedtuple
from typing import Optional, Tuple

DataFile = namedtuple(
    "DataFile",
    [
        "full_taiga_id",
        "underlying_taiga_id",
        "raw_path",
        "feather_path",
        "datafile_type",
    ],
)


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
                underlying_taiga_id TEXT,
                raw_path TEXT,
                feather_path TEXT,
                datafile_type TEXT
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

    def get_entry(self, queried_taiga_id: str) -> Optional[DataFile]:
        c = self.conn.cursor()
        c.execute(
            """
            SELECT datafiles.full_taiga_id, underlying_taiga_id, raw_path, feather_path, datafile_type
            FROM datafiles
            LEFT JOIN aliases
            ON
                datafiles.full_taiga_id = aliases.full_taiga_id
            WHERE
                alias = ? OR
                datafiles.full_taiga_id = ?
            """,
            (queried_taiga_id, queried_taiga_id),
        )

        r = c.fetchone()
        c.close()
        if r is None:
            return None

        datafile = DataFile(*r)
        if datafile.underlying_taiga_id is not None:
            return self.get_entry(datafile.underlying_taiga_id)
        return datafile

    def get_entry_path_and_format(
        self, queried_taiga_id: str
    ) -> Tuple[Optional[str], Optional[str]]:
        entry = self.get_entry(queried_taiga_id)
        if entry is not None:
            return entry.feather_path, entry.datafile_type
        return None, None

    def get_full_taiga_id(self, queried_taiga_id: str) -> Optional[str]:
        entry = self.get_entry(queried_taiga_id)
        if entry is not None:
            return entry.full_taiga_id
        return None

    def get_path_and_make_directories(
        self,
        dataset_permaname: str,
        dataset_version: str,
        datafile_name: str,
        extension: Optional[str] = None,
    ) -> str:
        file_path = os.path.join(
            self.cache_dir,
            dataset_permaname,
            dataset_version,
            "{}.{}".format(datafile_name, extension)
            if extension is not None
            else datafile_name,
        )
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        return file_path

    def add_entry(
        self,
        full_taiga_id: str,
        raw_path: Optional[str],
        feather_path: Optional[str],
        datafile_type: str,
    ):
        c = self.conn.cursor()
        entry = self.get_entry(full_taiga_id)
        if entry is None:
            c.execute(
                """
                INSERT INTO datafiles
                VALUES (?, ?, ?, ?, ?)
                """,
                (full_taiga_id, None, raw_path, feather_path, datafile_type),
            )
        elif entry.feather_path is None:
            c.execute(
                """
                UPDATE datafiles
                SET feather_path = ?,
                    datafile_type = ?
                WHERE
                    full_taiga_id = ?
                """,
                (feather_path, datafile_type, full_taiga_id),
            )
        c.close()
        self.conn.commit()

    def add_alias(self, queried_taiga_id: str, full_taiga_id: str):
        c = self.conn.cursor()
        entry = self.get_entry(queried_taiga_id)
        if entry is None:
            c.execute(
                """
                INSERT INTO aliases
                VALUES (?, ?)
                """,
                (queried_taiga_id, full_taiga_id),
            )

    def add_virtual_datafile(self, full_taiga_id: str, underlying_taiga_id: str):
        c = self.conn.cursor()
        entry = self.get_entry(full_taiga_id)
        if entry is None:
            c.execute(
                """
                INSERT INTO datafiles
                VALUES (?, ?, ?, ?, ?)
                """,
                (full_taiga_id, underlying_taiga_id, None, None, None),
            )
        c.close()
        self.conn.commit()

    def update_datafile(
        self,
        full_taiga_id: str,
        raw_path: Optional[str] = None,
        feather_path: Optional[str] = None,
        datafile_type: Optional[str] = None,
    ):
        entry = self.get_entry(full_taiga_id)
        c = self.conn.cursor()

        c.execute(
            """
            UPDATE datafiles
            SET raw_path = ?,
                feather_path = ?,
                datafile_type = ?
            WHERE
                full_taiga_id = ?
            """,
            (
                raw_path if raw_path is not None else entry.raw_path,
                feather_path if feather_path is not None else entry.raw_path,
                datafile_type if datafile_type is not None else entry.raw_path,
                full_taiga_id,
            ),
        )
        c.close()
        self.conn.commit()

    def remove_from_cache(self, queried_taiga_id: str):
        raise NotImplementedError
