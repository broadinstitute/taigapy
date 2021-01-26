__version__ = "3.0.3"


import asyncio
import os
import tempfile

from collections import defaultdict
from typing import Collection, DefaultDict, List, Optional, Tuple, Union

import aiobotocore
import colorful as cf
import pandas as pd

from taigapy.taiga_api import TaigaApi
from taigapy.taiga_cache import TaigaCache
from taigapy.figshare import parse_figshare_map_file, download_file_from_figshare
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
    S3Credentials,
    UploadDataFile,
    UploadS3DataFileDict,
    UploadS3DataFile,
    UploadVirtualDataFileDict,
    UploadVirtualDataFile,
)
from taigapy.custom_exceptions import (
    TaigaDeletedVersionException,
    Taiga404Exception,
    TaigaCacheFileCorrupted,
    TaigaRawTypeException,
)

DEFAULT_TAIGA_URL = "https://cds.team/taiga"

# global variable to allow people to globally override the location before initializing client
# which is often useful in adhoc scripts being submitted onto the cluster.
DEFAULT_CACHE_DIR = "~/.taiga"
CACHE_FILE = ".cache.db"


class TaigaClient:
    def __init__(
        self,
        url: str = DEFAULT_TAIGA_URL,
        cache_dir: Optional[str] = None,
        token_path: Optional[str] = None,
        figshare_map_file: Optional[str] = None,
    ):
        self.url = url
        self.token = None
        self.token_path = token_path
        self.api: TaigaApi = None

        if cache_dir is None:
            cache_dir = DEFAULT_CACHE_DIR
        self.cache_dir = os.path.expanduser(cache_dir)

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        cache_file_path = os.path.join(self.cache_dir, CACHE_FILE)
        self.cache = TaigaCache(self.cache_dir, cache_file_path)

        if figshare_map_file is not None:
            self.figshare_map = parse_figshare_map_file(figshare_map_file)
        else:
            self.figshare_map = None

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
            dataset_metadata: DatasetMetadataDict = self.api.get_dataset_version_metadata(
                dataset_name, None
            )
            dataset_version = get_latest_valid_version_from_metadata(dataset_metadata)
            print(
                cf.orange(
                    "No dataset version provided. Using version {}.".format(
                        dataset_version
                    )
                )
            )

        metadata = self.api.get_datafile_metadata(
            id_or_permaname, dataset_name, dataset_version, datafile_name
        )

        if metadata is None:
            raise ValueError(
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
    ) -> Union[str, pd.DataFrame]:
        with tempfile.NamedTemporaryFile(dir=self.cache_dir, delete=False) as tf:
            dataset_permaname = datafile_metadata.dataset_permaname
            dataset_version = datafile_metadata.dataset_version
            datafile_name = datafile_metadata.datafile_name
            datafile_format = datafile_metadata.datafile_format

            self.api.download_datafile(
                dataset_permaname, dataset_version, datafile_name, tf.name
            )

            if not get_dataframe:
                return self.cache.add_raw_entry(
                    tf.name,
                    query,
                    full_taiga_id,
                    DataFileFormat(datafile_metadata.datafile_format),
                )

            column_types = None
            if datafile_format == DataFileFormat.Columnar:
                column_types = self.api.get_column_types(
                    dataset_permaname, dataset_version, datafile_name
                )

            return self.cache.add_entry(
                tf.name,
                query,
                full_taiga_id,
                datafile_format,
                column_types,
                datafile_metadata.datafile_encoding,
            )

    def _get_dataframe_or_path_from_figshare(self, taiga_id: str, get_dataframe: bool):
        if taiga_id in self.figshare_map:
            figshare_file_metadata = self.figshare_map[taiga_id]
        else:
            raise ValueError("{} is not in figshare_file_map".format(taiga_id))

        if get_dataframe and figshare_file_metadata["format"] == DataFileFormat.Raw:
            raise ValueError(
                "The file is a Raw one, please use instead `download_to_cache` with the same parameters"
            )

        get_from_cache = (
            self.cache.get_entry if get_dataframe else self.cache.get_raw_path
        )
        d = get_from_cache(taiga_id, taiga_id)

        if d is not None:
            return d

        with tempfile.NamedTemporaryFile(dir=self.cache_dir, delete=False) as tf:
            download_file_from_figshare(figshare_file_metadata["download_url"], tf.name)

            if not get_dataframe:
                return self.cache.add_raw_entry(
                    tf.name, taiga_id, taiga_id, figshare_file_metadata["format"]
                )
            else:
                return self.cache.add_entry(
                    tf.name,
                    taiga_id,
                    taiga_id,
                    figshare_file_metadata["format"],
                    figshare_file_metadata.get("column_types"),
                    figshare_file_metadata.get("encoding"),
                )

    def _get_dataframe_or_path(
        self,
        id: Optional[str],
        name: Optional[str],
        version: Optional[DatasetVersion],
        file: Optional[str],
        get_dataframe: bool,
    ) -> Optional[Union[str, pd.DataFrame]]:
        if self.figshare_map is not None:
            try:
                return self._get_dataframe_or_path_from_figshare(id, get_dataframe)
            except ValueError as e:
                print(cf.red(str(e)))
                return None

        self._set_token_and_initialized_api()

        if not self.api.is_connected():
            return self._get_dataframe_or_path_offline(
                id, name, version, file, get_dataframe
            )

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
        except TaigaCacheFileCorrupted as e:
            print(cf.orange(str(e)))

        # Download from Taiga
        try:
            return self._download_file_and_save_to_cache(
                query, full_taiga_id, datafile_metadata, get_dataframe
            )
        except (Taiga404Exception, ValueError) as e:
            print(cf.red(str(e)))
            return None

    def _get_dataframe_or_path_offline(
        self,
        id: Optional[str],
        name: Optional[str],
        version: Optional[DatasetVersion],
        file: Optional[str],
        get_dataframe: bool,
    ):
        print(
            cf.orange(
                "You are in offline mode, please be aware that you might be out of sync with the state of the dataset version (deprecation)."
            )
        )
        if id is not None:
            query = id
        else:
            query = format_datafile_id(name, version, file)

        get_from_cache = (
            self.cache.get_entry if get_dataframe else self.cache.get_raw_path
        )

        try:
            df_or_path = get_from_cache(query, query)
            if df_or_path is not None:
                return df_or_path
        except TaigaRawTypeException as e:
            print(
                cf.red(
                    "The file is a Raw one, please use instead `download_to_cache` with the same parameters"
                )
            )
            return None
        except TaigaCacheFileCorrupted as e:
            print(cf.red(str(e)))
            return None

        print(cf.red("The datafile you requested was not in the cache."))
        return None

    def _validate_upload_files(
        self,
        upload_files: Collection[UploadS3DataFileDict],
        add_taiga_ids: Collection[UploadVirtualDataFileDict],
        previous_version_taiga_ids: Optional[
            Collection[UploadVirtualDataFileDict]
        ] = None,
    ) -> Tuple[List[UploadS3DataFile], List[UploadVirtualDataFile]]:
        upload_s3_datafiles = [UploadS3DataFile(f) for f in upload_files]
        upload_virtual_datafiles = [UploadVirtualDataFile(f) for f in add_taiga_ids]
        previous_version_datafiles = (
            [UploadVirtualDataFile(f) for f in previous_version_taiga_ids]
            if previous_version_taiga_ids is not None
            else None
        )

        # https://github.com/python/typeshed/issues/2383
        all_upload_datafiles: Collection[
            UploadDataFile
        ] = upload_s3_datafiles + upload_virtual_datafiles  # type: ignore

        datafile_names: DefaultDict[str, int] = defaultdict(int)
        for upload_datafile in all_upload_datafiles:
            datafile_names[upload_datafile.file_name] += 1

        duplicate_file_names = [
            file_name for file_name, count in datafile_names.items() if count > 1
        ]
        if len(duplicate_file_names) > 0:
            raise ValueError(
                "Multiple files named {}.".format(", ".join(duplicate_file_names))
            )

        if previous_version_taiga_ids is not None:
            for upload_datafile in previous_version_datafiles:
                if upload_datafile.file_name not in datafile_names:
                    upload_virtual_datafiles.append(upload_datafile)

        return upload_s3_datafiles, upload_virtual_datafiles

    def _validate_create_dataset_arguments(
        self,
        dataset_name: str,
        upload_files: Optional[Collection[UploadS3DataFileDict]],
        add_taiga_ids: Optional[Collection[UploadVirtualDataFileDict]],
        folder_id: Optional[str],
    ):
        if len(dataset_name) == 0:
            raise ValueError("dataset_name must be a nonempty string.")
        if len(upload_files) == 0 and len(add_taiga_ids) == 0:
            raise ValueError("upload_files and add_taiga_ids cannot both be empty.")

        upload_s3_datafiles, upload_virtual_datafiles = self._validate_upload_files(
            upload_files, add_taiga_ids
        )

        try:
            self.api.get_folder(folder_id)
        except Taiga404Exception:
            raise ValueError("No folder found with id {}.".format(folder_id))

        return upload_s3_datafiles, upload_virtual_datafiles

    def _validate_update_dataset_arguments(
        self,
        dataset_id: Optional[str],
        dataset_permaname: Optional[str],
        dataset_version: Optional[DatasetVersion],
        changes_description: Optional[str],
        upload_files: Optional[Collection[UploadS3DataFileDict]],
        add_taiga_ids: Optional[Collection[UploadVirtualDataFileDict]],
        add_all_existing_files: bool,
    ) -> Tuple[
        List[UploadS3DataFile], List[UploadVirtualDataFile], DatasetVersionMetadataDict
    ]:
        if dataset_id is None and dataset_permaname is None:
            # TODO standardize exceptions
            raise ValueError("Dataset id or name must be specified.")

        if changes_description is None or changes_description == "":
            raise ValueError("Description of changes cannot be empty.")

        if dataset_id is not None:
            if "." in dataset_id:
                (
                    dataset_permaname,
                    dataset_version,
                    _,
                ) = untangle_dataset_id_with_version(dataset_id)
            else:
                dataset_metadata: DatasetMetadataDict = self.get_dataset_metadata(
                    dataset_id
                )
                dataset_permaname = dataset_metadata["permanames"][-1]
        else:
            dataset_metadata = self.get_dataset_metadata(dataset_permaname)

        if dataset_version is None:
            dataset_version = get_latest_valid_version_from_metadata(dataset_metadata)
            print(
                cf.orange(
                    "No dataset version provided. Using version {}.".format(
                        dataset_version
                    )
                )
            )

        dataset_version_metadata: DatasetVersionMetadataDict = self.get_dataset_metadata(
            dataset_permaname, dataset_version
        )

        if add_all_existing_files:
            existing_files: List[UploadVirtualDataFileDict] = [
                {
                    "taiga_id": format_datafile_id(
                        dataset_permaname, dataset_version, datafile["name"]
                    )
                }
                for datafile in dataset_version_metadata["datasetVersion"]["datafiles"]
            ]
        else:
            existing_files = None

        upload_s3_datafiles, upload_virtual_datafiles = self._validate_upload_files(
            upload_files, add_taiga_ids, existing_files
        )

        return upload_s3_datafiles, upload_virtual_datafiles, dataset_version_metadata

    async def _upload_to_s3_and_request_conversion(
        self,
        session_id: str,
        upload_file: UploadS3DataFile,
        s3_credentials: S3Credentials,
    ):
        print("Uploading {} to S3".format(upload_file.file_name))
        bucket = s3_credentials.bucket
        partial_prefix = s3_credentials.prefix
        key = "{}{}/{}".format(partial_prefix, session_id, upload_file.file_name)

        session = aiobotocore.get_session()
        async with session.create_client(
            "s3",
            aws_access_key_id=s3_credentials.access_key_id,
            aws_secret_access_key=s3_credentials.secret_access_key,
            aws_session_token=s3_credentials.session_token,
        ) as s3_client:
            with open(upload_file.file_path, "rb") as f:
                resp = await s3_client.put_object(Bucket=bucket, Key=key, Body=f.read())
        upload_file.add_s3_upload_information(bucket, key)
        print("Finished uploading {} to S3".format(upload_file.file_name))

        print("Uploading {} to Taiga".format(upload_file.file_name))
        await self.api.upload_file_to_taiga(session_id, upload_file)
        print("Finished uploading {} to Taiga".format(upload_file.file_name))

    async def _upload_files(
        self,
        upload_s3_datafiles: List[UploadS3DataFile],
        upload_virtual_datafiles: List[UploadVirtualDataFile],
    ) -> str:
        upload_session_id = self.api.create_upload_session()
        s3_credentials = self.api.get_s3_credentials()

        tasks = [
            asyncio.ensure_future(
                self._upload_to_s3_and_request_conversion(
                    upload_session_id, f, s3_credentials
                )
            )
            for f in upload_s3_datafiles
        ]
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            for t in tasks:
                t.cancel()
            raise e

        for upload_file in upload_virtual_datafiles:
            print("Linking virtual file {}".format(upload_file.taiga_id))
            await self.api.upload_file_to_taiga(upload_session_id, upload_file)

        return upload_session_id

    # User-facing functions
    def get(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[DatasetVersion] = None,
        file: Optional[str] = None,
    ) -> pd.DataFrame:
        """Retrieves a Table or NumericMatrix datafile from Taiga (or local cache, if available) and returns it as a pandas.DataFrame.

        Stores the file in the cache if it is not already stored.

        Errors if the requested datafile is not a Table or NumericMatrix (i.e. is a Raw datafile).

        If used while offline, will get datafiles that are already in the cache.

        Keyword Arguments:
            id {Optional[str]} -- Datafile ID of the datafile to get, in the form dataset_permaname.dataset_version/datafile_name, or dataset_permaname.dataset_version if there is only one file in the dataset. Required if dataset_name is not provided. Takes precedence if both are provided. (default: {None})
            name {Optional[str]} -- Permaname or id of the dataset with the datafile. Required if id is not provided. Not used if both are provided. (default: {None})
            version {Optional[Union[str, int]]} -- Version of the dataset. If not provided, will use the latest approved (i.e. not deprecated or deleted) dataset. Required if id is not provided. Not used if both are provided. (default: {None})
            file {Optional[str]} -- Name of the datafile in the dataset. Required if id is not provided and the dataset contains more than one file. Not used if id is provided. (default: {None})

        Returns:
            pd.DataFrame -- If the file is a NumericMatrix, the row headers will be used as the DataFrame's index.
        """
        return self._get_dataframe_or_path(id, name, version, file, get_dataframe=True)

    def download_to_cache(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        version: Optional[DatasetVersion] = None,
        file: Optional[str] = None,
    ) -> str:
        """Retrieves a datafile from Taiga in its raw format (CSV or plain text file).

        Keyword Arguments:
            id {Optional[str]} -- Datafile ID of the datafile to get, in the form dataset_permaname.dataset_version/datafile_name, or dataset_permaname.dataset_version if there is only one file in the dataset. Required if dataset_name is not provided. Takes precedence if both are provided. (default: {None})
            name {Optional[str]} -- Permaname or id of the dataset with the datafile. Required if id is not provided. Not used if both are provided. (default: {None})
            version {Optional[Union[str, int]]} -- Version of the dataset. If not provided, will use the latest approved (i.e. not deprecated or deleted) dataset. Required if id is not provided. Not used if both are provided. (default: {None})
            file {Optional[str]} -- Name of the datafile in the dataset. Required if id is not provided and the dataset contains more than one file. Not used if id is provided. (default: {None})

        Returns:
            str -- The path of the downloaded file.
        """
        return self._get_dataframe_or_path(id, name, version, file, get_dataframe=False)

    def get_dataset_metadata(
        self, dataset_id: str, version: Optional[DatasetVersion] = None
    ) -> Union[DatasetMetadataDict, DatasetVersionMetadataDict]:
        """Get metadata about a dataset
        
        Keyword Arguments:
            id {Optional[str]} -- Datafile ID of the datafile to get, in the form dataset_permaname.dataset_version/datafile_name, or dataset_permaname.dataset_version if there is only one file in the dataset. Required if dataset_name is not provided. Takes precedence if both are provided. (default: {None})
            name {Optional[str]} -- Permaname or id of the dataset with the datafile. Required if id is not provided. Not used if both are provided. (default: {None})
            version {Optional[Union[str, int]]} -- Version of the dataset. If not provided, will use the latest approved (i.e. not deprecated or deleted) dataset. Required if id is not provided. Not used if both are provided. (default: {None})
            file {Optional[str]} -- Name of the datafile in the dataset. Required if id is not provided and the dataset contains more than one file. Not used if id is provided. (default: {None})

        Returns:
            Union[DatasetMetadataDict, DatasetVersionMetadataDict] -- See docs at https://github.com/broadinstitute/taigapy for more details
        """
        self._set_token_and_initialized_api()

        if "." in dataset_id:
            try:
                dataset_id, version, _ = untangle_dataset_id_with_version(dataset_id)
            except ValueError as e:
                print(cf.red(str(e)))
                return None

        try:
            return self.api.get_dataset_version_metadata(dataset_id, version)
        except (Taiga404Exception) as e:
            print(cf.red(str(e)))
            return None

    def create_dataset(
        self,
        dataset_name: str,
        dataset_description: Optional[str] = None,
        upload_files: Optional[Collection[UploadS3DataFileDict]] = None,
        add_taiga_ids: Optional[Collection[UploadVirtualDataFileDict]] = None,
        folder_id: str = None,
    ) -> Optional[str]:
        """Creates a new dataset named dataset_name with local files upload_files and virtual datafiles add_taiga_ids in the folder with id parent_folder_id.

        If multiple files in the union of upload_files and add_taiga_ids share the same name, Taiga will throw and error and the dataset will not be created.

        Arguments:
            dataset_name {str} -- The name of the new dataset.

        Keyword Arguments:
            dataset_description {Optional[str]} -- Description of the dataset. (default: {None})
            upload_files {Optional[List[Dict[str, str]]]} -- List of files to upload, where files are provided as dictionary objects d where
                - d["path"] is the path of the file to upload
                - d["name"] is what the file should be named in the dataset. Uses the base name of the file if not provided
                - d["format"] is the Format of the file (as a string).
                And optionally,
                - d["encoding"] is the character encoding of the file. Uses "UTF-8" if not provided
                (default: {None})
            add_taiga_ids {Optional[List[Dict[str, str]]]} -- List of virtual datafiles to add, where files are provided as dictionary objects with keys
                - "taiga_id" equal to the Taiga ID of the reference datafile in dataset_permaname.dataset_version/datafile_name format
                - "name" (optional) for what the virtual datafile should be called in the new dataset (will use the reference datafile name if not provided).
                (default: {None})
            folder_id {str} -- The ID of the containing folder. If not specified, will use home folder of user. (default: {None})

        Returns:
            Optional[str] -- The id of the new dataset, or None if the operation was not successful.
        """
        self._set_token_and_initialized_api()

        if upload_files is None:
            upload_files = []
        if add_taiga_ids is None:
            add_taiga_ids = []

        try:
            if folder_id is None:
                folder_id = self.api.get_user()["home_folder_id"]
            (
                upload_s3_datafiles,
                upload_virtual_datafiles,
            ) = self._validate_create_dataset_arguments(
                dataset_name, upload_files, add_taiga_ids, folder_id
            )
        except ValueError as e:
            print(cf.red(str(e)))
            return None

        if upload_s3_datafiles is None:
            # User declined to upload to public folder
            return None

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            upload_session_id = loop.run_until_complete(
                self._upload_files(upload_s3_datafiles, upload_virtual_datafiles)
            )
            loop.close()
        except ValueError as e:
            print(cf.red(str(e)))
            return None

        dataset_id = self.api.create_dataset(
            upload_session_id, folder_id, dataset_name, dataset_description
        )
        print(
            cf.green(
                "Dataset created. Access it directly with this url: {}\n".format(
                    self.url + "/dataset/" + dataset_id
                )
            )
        )
        return dataset_id

    def update_dataset(
        self,
        dataset_id: Optional[str] = None,
        dataset_permaname: Optional[str] = None,
        dataset_version: Optional[DatasetVersion] = None,
        dataset_description: Optional[str] = None,
        changes_description: Optional[str] = None,
        upload_files: Optional[Collection[UploadS3DataFileDict]] = None,
        add_taiga_ids: Optional[Collection[UploadVirtualDataFileDict]] = None,
        add_all_existing_files: bool = False,
    ) -> str:
        """Creates a new version of dataset specified by dataset_id or dataset_name (and optionally dataset_version).

        Keyword Arguments:
            dataset_id {Optional[str]} -- Generated id or id in the format dataset_permaname.dataset_version (default: {None})
            dataset_permaname {Optional[str]} -- Permaname of the dataset to update. Must be provided if `dataset_id` is not (default: {None})
            dataset_version {Optional[Union[str, int]]} -- Dataset version to base the new version off of. If not specified, will use the latest version. (default: {None})
            dataset_description {Optional[str]} -- Description of new dataset version. Uses previous version's description if not specified. (default: {None})
            changes_description {Optional[str]} -- Description of changes new to this version, required. (default: {None})
            upload_files {Optional[List[Dict[str, str]]]} -- List of files to upload, where files are provided as dictionary objects d where
                - d["path"] is the path of the file to upload
                - d["name"] is what the file should be named in the dataset. Uses the base name of the file if not provided
                - d["format"] is the Format of the file (as a string).
                And optionally,
                - d["encoding"] is the character encoding of the file. Uses "UTF-8" if not provided
                (default: {None})
            add_taiga_ids {Optional[List[Dict[str, str]]]} -- List of virtual datafiles to add, where files are provided as dictionary objects with keys
                - "taiga_id" equal to the Taiga ID of the reference datafile in dataset_permaname.dataset_version/datafile_name format
                - "name" (optional) for what the virtual datafile should be called in the new dataset (will use the reference datafile name if not provided).
                (default: {None})
            add_all_existing_files {bool} -- Whether to add all files from the base dataset version as virtual datafiles in the new dataset version. If a name collides with one in upload_files or add_taiga_ids, that file is ignored. (default: {False})

        Returns:
            Optional[str] -- The id of the new dataset version, or None if the operation was not successful.
        """
        self._set_token_and_initialized_api()

        if upload_files is None:
            upload_files = []
        if add_taiga_ids is None:
            add_taiga_ids = []

        try:
            (
                upload_s3_datafiles,
                upload_virtual_datafiles,
                dataset_version_metadata,
            ) = self._validate_update_dataset_arguments(
                dataset_id,
                dataset_permaname,
                dataset_version,
                changes_description,
                upload_files,
                add_taiga_ids,
                add_all_existing_files,
            )
        except ValueError as e:
            print(cf.red(str(e)))
            return None

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            upload_session_id = loop.run_until_complete(
                self._upload_files(upload_s3_datafiles, upload_virtual_datafiles)
            )
            loop.close()
        except ValueError as e:
            print(cf.red(str(e)))
            return None

        dataset_description = (
            dataset_description
            if dataset_description is not None
            else dataset_version_metadata["datasetVersion"]["description"]
        )

        new_dataset_version_id = self.api.update_dataset(
            dataset_version_metadata["dataset"]["id"],
            upload_session_id,
            dataset_description,
            changes_description,
        )

        print(
            cf.green(
                "Dataset version created. Access it directly with this url: {}".format(
                    self.url + "/dataset_version/" + new_dataset_version_id
                )
            )
        )

        return new_dataset_version_id

    def get_canonical_id(self, queried_taiga_id: str) -> Optional[str]:
        """Get the canonical Taiga ID of a datafile specified by queried_taiga_id.

        A canonical ID is of the form dataset_permaname.dataset_version/datafile_name.

        If the datafile specified by queried_taiga_id is a virtual datafile, the canonical ID is that of the underlying datafile. 

        Arguments:
            queried_taiga_id {str} -- Taiga ID in the form dataset_permaname.dataset_version/datafile_name or dataset_permaname.dataset_version

        Returns:
            Optional[str] -- The canonical ID, or None if no datafile was found.
        """
        self._set_token_and_initialized_api()

        full_taiga_id = self.cache.get_full_taiga_id(queried_taiga_id)
        if full_taiga_id is not None:
            return full_taiga_id

        try:
            if "." in queried_taiga_id:
                (
                    dataset_permaname,
                    dataset_version,
                    datafile_name,
                ) = untangle_dataset_id_with_version(queried_taiga_id)
                datafile_metadata = self.api.get_datafile_metadata(
                    None, dataset_permaname, dataset_version, datafile_name
                )
            else:
                datafile_metadata = self.api.get_datafile_metadata(
                    queried_taiga_id, None, None, None
                )
        except Taiga404Exception as e:
            print(cf.red(str(e)))
            return None

        dataset_version_metadata: DatasetVersionMetadataDict = self.get_dataset_metadata(
            format_datafile_id_from_datafile_metadata(datafile_metadata)
        )

        # Add canonical IDs for all other files in dataset, while we're at it
        for f in dataset_version_metadata["datasetVersion"]["datafiles"]:
            datafile_id = format_datafile_id(
                datafile_metadata.dataset_permaname,
                datafile_metadata.dataset_version,
                f["name"],
            )

            real_datafile_id = (
                datafile_id
                if "underlying_file_id" not in f
                else f["underlying_file_id"]
            )

            self.cache.add_full_id(
                datafile_id, real_datafile_id, DataFileFormat(f["type"])
            )

            if f["name"] == datafile_metadata.datafile_name:
                self.cache.add_full_id(
                    queried_taiga_id,
                    real_datafile_id,
                    datafile_metadata.datafile_format,
                )

        return self.cache.get_full_taiga_id(queried_taiga_id)


try:
    default_tc = TaigaClient()
except Exception as e:
    print("default_tc could not be set for this reason: {}".format(e))
    print(
        "You can import TaigaClient and add your custom options if you would want to customize it to your settings"
    )
