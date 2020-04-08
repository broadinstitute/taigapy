import os
import tempfile

from typing import Optional, Union

import colorful as cf
import pandas as pd

from taigapy.taiga_api import TaigaApi
from taigapy.taiga_cache import TaigaCache
from taigapy.utils import (
    find_first_existing,
    format_datafile_id,
    format_datafile_id_from_datafile_metadata,
    untangle_dataset_id_with_version,
    get_latest_valid_version_from_metadata,
)
from taigapy.types import (
    DataFileFormat,
    DatasetVersion,
    DatasetVersionState,
    DatasetMetadataDict,
    DatasetVersionMetadataDict,
    DataFileMetadata,
)
from taigapy.custom_exceptions import TaigaDeletedVersionException

__version__ = "TODO"

DEFAULT_TAIGA_URL = "https://cds.team/taiga"

# global variable to allow people to globally override the location before initializing client
# which is often useful in adhoc scripts being submitted onto the cluster.
DEFAULT_CACHE_DIR = "~/.taiga"
CACHE_FILE = ".cache.db"


class TaigaClient:
    def __init__(self, url=DEFAULT_TAIGA_URL, cache_dir=None, token_path=None):
        self.url = url
        self.token = None
        self.token_path = token_path
        self.api = None

        if cache_dir is None:
            cache_dir = DEFAULT_CACHE_DIR
        self.cache_dir = os.path.expanduser(cache_dir)

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        cache_file_path = os.path.join(self.cache_dir, CACHE_FILE)
        self.cache = TaigaCache(self.cache_dir, cache_file_path)

    def _set_token_and_initialized_api(self):
        if self.token is not None:
            return

        if self.token_path is None:
            token_path = find_first_existing(
                ["./.taiga-token", os.path.join(self.cache_dir, "token")]
            )
        else:
            token_path = find_first_existing([self.token_path])

        with open(token_path, "rt") as r:
            self.token = r.readline().strip()
        self.api = TaigaApi(self.url, self.token)

    def _validate_file_for_download(
        self,
        id_or_permaname: Optional[str],
        dataset_name: Optional[str],
        dataset_version: Optional[str],
        datafile_name: Optional[str],
    ) -> DataFileMetadata:
        if id_or_permaname is None and dataset_name is None:
            # TODO standardize exceptions
            raise ValueError("id or name must be specified")
        elif (
            id_or_permaname is None
            and dataset_name is not None
            and dataset_version is None
        ):
            dataset_metadata = self.api.get_dataset_version_metadata(
                dataset_name, dataset_version
            )
            dataset_version = get_latest_valid_version_from_metadata(dataset_metadata)
            print(
                cf.orange(
                    "No dataset version provided. Using version {}.".format(
                        dataset_version
                    )
                )
            )
        elif (
            id_or_permaname is not None
            and datafile_name is None
            and "." not in id_or_permaname
        ):
            raise ValueError(
                "id not in the format dataset_permaname.version/datafile_name"
            )

        metadata = self.api.get_datafile_metadata(
            id_or_permaname, dataset_name, dataset_version, datafile_name
        )

        if metadata is None:
            raise Exception(
                "No data for the given parameters. Please check your inputs are correct."
            )

        dataset_version_id = metadata.dataset_version_id
        dataset_permaname = metadata.dataset_permaname
        dataset_version = metadata.dataset_version
        datafile_name = metadata.datafile_name
        data_state = metadata.state
        data_reason_state = metadata.reason_state

        assert dataset_version_id is not None
        assert dataset_permaname is not None
        assert dataset_version is not None
        assert datafile_name is not None

        if data_state == DatasetVersionState.deprecated.value:
            print(
                cf.orange(
                    "WARNING: This version is deprecated. Please use with caution, and see the reason below:"
                )
            )
            print(cf.orange("\t{}".format(data_reason_state)))
        elif data_state == DatasetVersionState.deleted.value:
            self.cache.remove_all_from_cache(
                "{}.{}/".format(dataset_permaname, dataset_version)
            )
            raise TaigaDeletedVersionException(
                "{} version {} is deleted. The data is not available anymore. Contact the maintainer of the dataset.".format(
                    dataset_permaname, dataset_version
                )
            )

        return metadata

    def _download_file_and_save_to_cache(
        self,
        query: str,
        full_taiga_id: str,
        datafile_metadata: DataFileMetadata,
        get_dataframe: bool,
    ):
        with tempfile.NamedTemporaryFile() as tf:
            dataset_permaname = datafile_metadata.dataset_permaname
            dataset_version = datafile_metadata.dataset_version
            datafile_name = datafile_metadata.datafile_name
            datafile_format = datafile_metadata.datafile_format

            self.api.download_datafile(
                dataset_permaname, dataset_version, datafile_name, tf.name
            )

            if datafile_format == DataFileFormat.Raw:
                self.cache.add_raw_entry(
                    tf.name,
                    query,
                    full_taiga_id,
                    DataFileFormat(datafile_metadata.datafile_format),
                )
                return

            column_types = None
            if datafile_format == DataFileFormat.Columnar:
                column_types = self.api.get_column_types(
                    dataset_permaname, dataset_version, datafile_name,
                )

            self.cache.add_entry(
                tf.name,
                query,
                full_taiga_id,
                datafile_format,
                column_types,
                datafile_metadata.datafile_encoding,
            )

    def _get_dataframe_or_path(
        self,
        id: Optional[str],
        name: Optional[str],
        version: Optional[DatasetVersion],
        file: Optional[str],
        get_dataframe: bool,
    ) -> Optional[Union[str, pd.DataFrame]]:
        self._set_token_and_initialized_api()
        # Validate inputs
        try:
            datafile_metadata = self._validate_file_for_download(
                id, name, str(version) if version is not None else version, file
            )
        except (TaigaDeletedVersionException, ValueError, Exception) as e:
            print(cf.red(str(e)))
            return None

        datafile_format = datafile_metadata.datafile_format
        if get_dataframe and datafile_format == DataFileFormat.Raw:
            print(
                cf.red(
                    "The file is a Raw one, please use instead `download_to_cache` with the same parameters"
                )
            )
            return None

        # Check the cache
        if id is not None:
            query = id
        else:
            query = format_datafile_id(name, version, file)

        full_taiga_id = format_datafile_id_from_datafile_metadata(datafile_metadata)
        if datafile_metadata.underlying_file_id is not None:
            full_taiga_id = datafile_metadata.underlying_file_id
        get_from_cache = (
            self.cache.get_entry if get_dataframe else self.cache.get_raw_path
        )
        try:
            df_or_path = get_from_cache(query, full_taiga_id)
            if df_or_path is not None:
                return df_or_path
        except Exception as e:
            print(cf.orange(str(e)))

        # Download from Taiga
        try:
            self._download_file_and_save_to_cache(
                query, full_taiga_id, datafile_metadata, get_dataframe
            )
            return get_from_cache(query, full_taiga_id)
        except Exception as e:
            print(cf.red(str(e)))
            return None

    # User-facing functions
    def get(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[DatasetVersion] = None,
        file: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._get_dataframe_or_path(id, name, version, file, get_dataframe=True)

    def download_to_cache(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[DatasetVersion] = None,
        file: Optional[str] = None,
    ) -> str:
        return self._get_dataframe_or_path(id, name, version, file, get_dataframe=False)

    def get_dataset_metadata(
        self, dataset_id: str, version: Optional[DatasetVersion] = None,
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        """Get metadata about a dataset"""
        self._set_token_and_initialized_api()
        if "." in dataset_id:
            try:
                dataset_id, version, _ = untangle_dataset_id_with_version(dataset_id)
            except ValueError as e:
                print(cf.red(str(e)))
                return None

        try:
            return self.api.get_dataset_version_metadata(dataset_id, version)
        except Exception as e:
            print(cf.red(str(e)))
            return None

    def create_dataset(
        self,
        dataset_name: str = None,
        dataset_description: str = None,
        upload_file_path_dict: Dict[str, str] = None,
        add_taiga_ids: List[Tuple[str, str]] = None,
        folder_id: str = None,
    ) -> Optional[str]:
        if upload_file_path_dict is None:
            upload_file_path_dict = {}
        if add_taiga_ids is None:
            add_taiga_ids = []

        if len(upload_file_path_dict) == 0 and len(add_taiga_ids) == 0:
            raise ValueError("TODO")


default_tc = TaigaClient()
