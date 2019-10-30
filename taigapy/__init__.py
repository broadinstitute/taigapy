import boto3
import colorful
import datetime
import glob
import h5py
import json
import numpy
import os
import pandas
import progressbar
import requests
import sys
import time
from typing import Dict, List, Tuple, Union

from taigapy.UploadFile import UploadFile
from taigapy.custom_exceptions import (
    TaigaHttpException,
    Taiga404Exception,
    TaigaDeletedVersionException,
    TaigaRawTypeException,
    TaigaClientConnectionException,
)

__version__ = "2.12.6"

DEFAULT_TAIGA_URL = "https://cds.team/taiga"

# global variable to allow people to globally override the location before initializing client
# which is often useful in adhoc scripts being submitted onto the cluster.
DEFAULT_CACHE_DIR = "~/.taiga"
VIRTUAL_UNDERLYING_MAP_FILE = ".virtual-underlying-map"


def read_hdf5(filename):
    src = h5py.File(filename, "r")
    try:
        dim_0 = [x.decode("utf8") for x in src["dim_0"]]
        dim_1 = [x.decode("utf8") for x in src["dim_1"]]
        data = numpy.array(src["data"])
        return pandas.DataFrame(index=dim_0, columns=dim_1, data=data).reset_index()
    finally:
        src.close()


class Taiga2Client:
    def __init__(self, url=DEFAULT_TAIGA_URL, cache_dir=None, token_path=None):
        self.formats = [
            "NumericMatrixCSV",
            "NumericMatrixTSV",
            "TableCSV",
            "TableTSV",
            "GCT",
            "Raw",
        ]
        self.url = url

        if cache_dir is None:
            cache_dir = DEFAULT_CACHE_DIR
        self.cache_dir = os.path.expanduser(cache_dir)

        if not os.path.exists(self.cache_dir):
            os.mkdir(self.cache_dir)

        self.virtual_underlying_map_path = os.path.join(
            self.cache_dir, VIRTUAL_UNDERLYING_MAP_FILE
        )
        if not os.path.exists(self.virtual_underlying_map_path):
            pandas.DataFrame(columns=["virtual", "underlying"]).to_csv(
                self.virtual_underlying_map_path, index=False
            )

        if token_path is None:
            token_path = self._find_first_existing(
                ["./.taiga-token", os.path.join(self.cache_dir, "token")]
            )

        with open(token_path, "rt") as r:
            self.token = r.readline().strip()

    def _find_first_existing(self, paths):
        for path in paths:
            if os.path.exists(path):
                return path
        raise Exception(
            "No token file found. Checked the following locations: {}".format(paths)
        )

    def _get_cache_partial_file_path(
        self, dataset_name: str, dataset_version: Union[str, int], datafile_name: str
    ) -> str:
        """
        Get the path (without file extension) of the file in the cache corresponding
        to dataset id `data_id` and file name `data_file`.
        """
        return os.path.join(
            self.cache_dir,
            "{}_v{}_{}".format(dataset_name, dataset_version, datafile_name),
        )

    def _get_cache_file_paths(
        self,
        dataset_name: str,
        dataset_version: Union[str, int],
        datafile_name: str,
        file_format: str,
    ):
        partial_path = self._get_cache_partial_file_path(
            dataset_name, dataset_version, datafile_name
        )
        file_path = "{}.{}".format(partial_path, file_format)
        temp_path = "{}.tmp".format(partial_path)
        feather_extra_path = "{}.featherextra".format(partial_path)
        return file_path, temp_path, feather_extra_path

    def _get_underlying_file_from_cache(self, virtual_file_partial, file_format):
        underlying_file_map = pandas.read_csv(
            self.virtual_underlying_map_path, index_col=0
        )
        if virtual_file_partial in underlying_file_map.index:
            partial_path = os.path.join(
                self.cache_dir,
                underlying_file_map.loc[virtual_file_partial]["underlying"],
            )
            file_path = "{}.{}".format(partial_path, file_format)
            temp_path = "{}.tmp".format(partial_path)
            feather_extra_path = "{}.featherextra".format(partial_path)
            return file_path, temp_path, feather_extra_path
        return None, None, None

    def _add_underlying_file_to_cache(self, virtual_file_path, underlying_file_path):
        underlying_file_map = pandas.read_csv(
            self.virtual_underlying_map_path, index_col=0
        )
        underlying_file_map.loc[virtual_file_path] = underlying_file_path
        underlying_file_map.to_csv(self.virtual_underlying_map_path)

    def _extract_datafile_metadata(
        self, dataset_name=None, dataset_version=None, datafile_name=None, metadata=None
    ):
        if metadata is None:
            metadata = self.get_dataset_metadata(dataset_name, version=dataset_version)
        datafile_metadata = next(
            f
            for f in metadata["datasetVersion"]["datafiles"]
            if f["name"] == datafile_name
        )
        return datafile_metadata

    def get_dataset_id_by_name(self, name, md5=None, version=None):
        """Deprecated"""
        params = dict(fetch="id", name=name)
        if md5 is not None:
            params["md5"] = md5
        if version is not None:
            params["version"] = str(version)

        r = requests.get(self.url + "/rest/v0/namedDataset", params=params)
        if r.status_code == 404:
            return None
        return r.text

    def _untangle_dataset_id_with_version(self, id):
        """File can be optional in the id"""
        name, version = id.split(".", 1)
        assert name
        assert version

        if "/" in version:
            version, file = version.split("/", 1)
            assert file
        else:
            file = None

        return name, version, file

    def _get_dataset_name_version_file(
        self,
        dataset_id: str,
        dataset_name: str,
        dataset_version: str,
        datafile_name: str,
    ):
        """Get the dataset name, dataset version, and datafile id from dataset id,
        name, and version and datafile name.
        
        Specifically, tries to parse `dataset_id` as 
        `dataset_name.dataset_version[/datafile_name]`, or returns `dataset_name`,
        `dataset_version`, and `datafile_name` arguments if not specified in 
        `dataset_id`.
        """
        if dataset_id is not None:
            if "." in dataset_id:
                (
                    dataset_name,
                    dataset_version,
                    maybe_datafile_name,
                ) = self._untangle_dataset_id_with_version(dataset_id)
                if maybe_datafile_name is not None:
                    datafile_name = maybe_datafile_name
        return dataset_name, dataset_version, datafile_name

    def _get_params_dict(self, id, name, version, file, force=None, format=None):
        """Parse the params into a dict we can use for GET/POST requests"""
        params = dict(format=format)

        name, version, file = self._get_dataset_name_version_file(
            id, name, version, file
        )

        if id is None and name is None:
            raise Exception("Either id or name should be provided")

        if name is not None:
            params["dataset_permaname"] = name
        else:
            params["dataset_version_id"] = id

        if version is not None:
            params["version"] = str(version)

        if file is not None:
            params["datafile_name"] = file

        if force:
            params["force"] = "Y"

        return params

    def _get_data_file_json(self, id, name, version, file, force, format):
        params = self._get_params_dict(
            id=id, name=name, version=version, file=file, force=force, format=format
        )

        api_endpoint = "/api/datafile"
        return self.request_get(api_endpoint, params)

    def _get_data_file_summary(self, id, name, version, file):
        """Get the summary of a datafile"""
        params = self._get_params_dict(id=id, name=name, version=version, file=file)

        api_endpoint = "/api/datafile/short_summary"
        return self.request_get(api_endpoint=api_endpoint, params=params)

    def _get_datafile_column_types(self, id, name, version, file):
        """Get column types for a Columnar datafile"""
        params = self._get_params_dict(id=id, name=name, version=version, file=file)
        api_endpoint = "/api/datafile/column_types"
        return self.request_get(api_endpoint=api_endpoint, params=params)

    def get_dataset_metadata(
        self,
        dataset_id: str = None,
        version: Union[str, int] = None,
        version_id: str = None,
    ) -> dict:
        """Get metadata about a dataset"""
        if dataset_id is None and version_id is None:
            raise Exception("Dataset name or dataset version ID must be provided")

        if dataset_id is not None and "." in dataset_id:
            dataset_id, version, _ = self._untangle_dataset_id_with_version(dataset_id)

        if dataset_id is not None:
            url = self.url + "/api/dataset/" + dataset_id
            if version is not None:
                url += "/" + str(version)
        else:
            url = self.url + "/api/datasetVersion/" + version_id

        r = requests.get(url, headers=dict(Authorization="Bearer " + self.token))
        assert r.status_code == 200
        return r.json()

    def get_datafile_types(self, dataset_id, version):
        r = requests.get(
            self.url + "/api/dataset/" + dataset_id + "/" + str(version),
            headers=dict(Authorization="Bearer " + self.token),
        )
        assert r.status_code == 200
        metadata = r.json()
        type_by_name = dict(
            [
                (datafile["name"], datafile["type"])
                for datafile in metadata["datasetVersion"]["datafiles"]
            ]
        )
        return type_by_name

    def _get_data_file_type(self, dataset_id, version, filename):
        type_by_name = self.get_datafile_types(dataset_id, version)
        return type_by_name[filename]

    def _get_dataset_version_id_from_permaname_version(self, name, version=None):
        assert (
            name
        ), "If not id is given, we need the permaname of the dataset and the version"
        api_endpoint_get_dataset_version_id = "/api/dataset/{datasetId}".format(
            datasetId=name
        )
        request = self.request_get(api_endpoint=api_endpoint_get_dataset_version_id)
        id = None

        versions = request["versions"]

        # Sort versions to get the latest at the end
        versions = sorted(versions, key=lambda x: int(x["name"]))

        if version is None:
            # If no version provided, we fetch the latest version
            id = versions[-1]["id"]
        else:
            for dataset_version in versions:
                if dataset_version["name"] == str(version):
                    id = dataset_version["id"]

        if not id:
            raise Exception(
                "The version {} does not exist in the dataset permaname {}".format(
                    version, name
                )
            )

        return id

    def _get_allowed_conversion_type_from_dataset_version(
        self, file, id=None, name=None, version=None
    ):
        # Get the id and version id of name/version
        if not id:
            # Need to get the dataset version from id
            id = self._get_dataset_version_id_from_permaname_version(
                name=name, version=version
            )
        # else:
        elif "." in id:
            name, version, file = self._get_dataset_name_version_file(
                id, name, version, file
            )
            if not name:
                raise Exception("Either id or name should be provided")
            if not version:
                raise Exception("No version found for this id {}".format(id))

            try:
                id = self._get_dataset_version_id_from_permaname_version(name, version)
            except:
                print(sys.exc_info()[0])

        api_endpoint = "/api/dataset/{datasetId}/{datasetVersionId}".format(
            datasetId=None, datasetVersionId=id
        )

        result = self.request_get(api_endpoint=api_endpoint)

        # Get the file
        full_dataset_version_datafiles = result["datasetVersion"]["datafiles"]

        # Get the allowed conversion type
        if not file:
            return full_dataset_version_datafiles[0]["allowed_conversion_type"]
        else:
            for datafile in full_dataset_version_datafiles:
                if datafile["name"] == file:
                    return datafile["allowed_conversion_type"]

        raise Taiga404Exception("Datafile not found...check the file name?")

    def _validate_file_for_download(self, id, name, version, file, force):
        if id is None:
            assert name is not None, "id or name must be specified"

        metadata = self._get_data_file_json(id, name, version, file, force, "metadata")
        if metadata is None:
            raise Exception(
                "No data for the given parameters. Please check your inputs are correct."
            )

        data_id = metadata["dataset_version_id"]
        data_name = metadata["dataset_permaname"]
        data_version = metadata["dataset_version"]
        data_file = metadata["datafile_name"]
        data_state = metadata["state"]
        data_reason_state = metadata["reason_state"]

        assert data_id is not None
        assert data_name is not None
        assert data_version is not None
        assert data_file is not None

        # TODO: Add enum to manage deprecation/approval/deletion
        if data_state == "Deprecated":
            print(
                colorful.orange(
                    "WARNING: This version is deprecated. Please use with caution, and see the reason below:"
                )
            )
            print(colorful.orange("\t{}".format(data_reason_state)))
        elif data_state == "Deleted":
            # We found a dataset version in deleted mode. Delete also the cache files
            partial_file_path = self._get_cache_partial_file_path(
                data_name, data_version, data_file
            )
            # remove all files with the pattern patial_file_path
            pattern_all_extensions = partial_file_path + ".*"
            file_list = glob.glob(pattern_all_extensions)
            for file_path in file_list:
                try:
                    # Double check we are removing only the files that are in the cache folder
                    if (
                        self.cache_dir in file_path
                        and data_id in file_path
                        and data_file in file_path
                    ):
                        os.remove(file_path)
                    else:
                        print(
                            "The file {} shouldn't be present in the deletion process. "
                            "Please contact the administrator of taigapy".format(
                                file_path
                            )
                        )
                except:
                    print("Error while deleting file: {}".format(file_path))

            raise TaigaDeletedVersionException(
                "This version is deleted. The data is not available anymore. Contact the maintainer of the dataset."
            )

        return data_id, data_name, data_version, data_file

    def _get_file_download_url(
        self,
        datafile_id: str,
        dataset_name: str,
        dataset_version: Union[str, int],
        datafile_name: str,
        force_convert: bool,
        file_format: str,
        quiet: bool,
    ) -> str:
        """Get the url to download a file and prints conversion status.
        
        If `quiet=True`, skips printing conversion status."""
        first_attempt = True
        prev_status = None
        delay_between_polls = 1
        waiting_for_conversion = True
        while waiting_for_conversion:
            response = self._get_data_file_json(
                datafile_id,
                dataset_name,
                dataset_version,
                datafile_name,
                force_convert,
                file_format,
            )
            force_convert = False

            if response.get("urls", None) is None:
                if not quiet:
                    if first_attempt:
                        print(
                            "Taiga needs to convert data before we can fetch it.  Waiting...\n"
                        )
                    else:
                        if prev_status != response["status"]:
                            print("Status: {}".format(response["status"]))

                prev_status = response["status"]
                first_attempt = False
                time.sleep(delay_between_polls)
            else:
                waiting_for_conversion = False

        urls = response["urls"]
        assert len(urls) == 1
        return urls[0]

    def _download_to_file_object(
        self,
        id: str,
        name: str,
        version: Union[str, int],
        file: str,
        force: bool,
        format: str,
        destination: str,
        quiet: bool,
    ):
        url = self._get_file_download_url(id, name, version, file, force, format, quiet)
        r = requests.get(url, stream=True)

        if not quiet:
            content_length = r.headers.get("Content-Length", None)
            if not content_length:
                content_length = progressbar.UnknownLength
            else:
                content_length = int(content_length)

            bar = self._progressbar_init(max_value=content_length)

        with open(destination, "wb") as handle:
            if not r.ok:
                raise Exception("Error fetching {}".format(url))

            # Stream by chunk of 100Kb
            chunk_size = 1024 * 100
            total = 0
            for block in r.iter_content(chunk_size):
                handle.write(block)

                total += chunk_size
                # total can be slightly superior to content_length
                if not quiet and (
                    content_length == progressbar.UnknownLength
                    or total <= content_length
                ):
                    bar.update(total)
            if not quiet:
                bar.finish()

    def _progressbar_init(self, max_value):
        """
        Initialize the progressbar object with the max_value passed as parameter
        :param max_value: int
        :return: ProgressBar
        """

        widgets = [
            progressbar.Bar(left="[", right="]"),
            progressbar.Percentage(),
            " | ",
            progressbar.FileTransferSpeed(),
            " | ",
            progressbar.DataSize(),
            " / ",
            progressbar.DataSize(variable="max_value"),
            " | ",
            progressbar.ETA(),
        ]
        bar = progressbar.ProgressBar(max_value=max_value, widgets=widgets)
        return bar

    def _download_to_local_file(
        self,
        datafile_id: str,
        datafile_name: str,
        force_fetch: bool,
        dest: str,
        force_convert: bool,
        file_format: str,
        quiet: bool,
    ):
        """
        Downloads a Taiga file to `dest` if it does not exist, or if `force_fetch=True`
        or `force_convert=True`.
        
        If `force_convert=True`, will tell Taiga to convert the file to `file_format`
        regardless of conversion status.
        """
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        if os.path.exists(dest) and (force_fetch or force_convert):
            os.remove(dest)

        if not os.path.exists(dest):
            self._download_to_file_object(
                datafile_id,
                None,
                None,
                datafile_name,
                force_convert,
                file_format,
                dest,
                quiet,
            )

    def download_to_cache(
        self, id=None, name=None, version=None, file=None, force=False, format="raw"
    ):
        """
        Downloads a taiga file of any format, if not already present, and returns the path to the file
        This file is not pickled
        """
        data_id, data_name, data_version, data_file = self._validate_file_for_download(
            id, name, version, file, force
        )
        file_path, _, _ = self._get_cache_file_paths(
            data_name, data_version, data_file, format
        )
        self._download_to_local_file(
            data_id, data_file, False, file_path, force, format, False
        )
        return file_path

    @staticmethod
    def read_feather_to_df(path: str, datafile_type: str) -> pandas.DataFrame:
        """Reads and returns a Pandas DataFrame from a Feather file at `path`.

        If `datafile_type` is "HDF5", we convert the first column to an index.
        """
        df = pandas.read_feather(path)
        if datafile_type == "HDF5":
            df.set_index(df.columns[0], inplace=True)
            df.index.name = None
        return df

    def _is_connected(self):
        try:
            requests.get(self.url)
            return True
        except requests.ConnectionError:
            return False

    @staticmethod
    def _get_existing_file_and_datafile_type(
        file_format: str, file_path: str, feather_extra_path: str
    ):
        if file_format == "raw":
            return file_path, "Raw"

        with open(feather_extra_path, "r") as f:
            feather_extra = json.load(f)
            datafile_type = feather_extra["datafile_type"]
        return file_path, datafile_type

    def _handle_download_to_cache_for_fetch_unconnected(
        self,
        dataset_id: str,
        dataset_name: str,
        dataset_version: Union[str, int],
        datafile_name: str,
        force_fetch: bool,
        force_convert: bool,
        file_format: str,
        quiet: bool,
        encoding: str,
    ):
        """Handles downloading/fetching from cache when there is no connection. Behaves
        as follows:

        If `force_fetch=True` or `force_convert=True`, raise an error, since we cannot
        get the file from Taiga without a connection.

        If `dataset_id` is of the form `DATASET_NAME.DATASET_VERSION/DATAFILE_NAME`,
        or if `dataset_name`, `dataset_version`, and `datafile_name` are all set, check
        the cache for existance. If it exists, return that path name and datafile type.
        Otherwise, raise an error.

        If we can't get the dataset name and version and the file name, raise an error
        since we'd have to make a request to Taiga to get them.
        """
        if force_fetch or force_convert:
            raise TaigaClientConnectionException(
                "ERROR: You are in offline mode. Cannot force fetch or convert."
            )

        (
            dataset_name,
            dataset_version,
            datafile_name,
        ) = self._get_dataset_name_version_file(
            dataset_id, dataset_name, dataset_version, datafile_name
        )
        if dataset_id is not None and dataset_name is None:
            raise TaigaClientConnectionException(
                "ERROR: You are in offline mode. Cannot determine dataset name and version for cache using datafile id '{}'.".format(
                    datafile_id
                )
            )

        if dataset_name is None:
            raise ValueError("ERROR: No dataset name provided")
        if dataset_version is None:
            raise ValueError("ERROR: No dataset version provided")
        if datafile_name is None:
            raise ValueError("ERROR: No datafile name provided")

        file_path, _, feather_extra_path = self._get_cache_file_paths(
            dataset_name, dataset_version, datafile_name, file_format
        )

        relative_partial_path = os.path.splitext(os.path.basename(file_path))[0]

        (
            underlying_file_path,
            _,
            underlying_feather_extra_path,
        ) = self._get_underlying_file_from_cache(relative_partial_path, file_format)
        if underlying_file_path:
            file_path = underlying_file_path
            feather_extra_path = underlying_feather_extra_path

        if os.path.exists(file_path) and (
            file_format == "raw" or os.path.exists(feather_extra_path)
        ):
            print(
                colorful.orange(
                    "WARNING: You are in offline mode, please be aware that you might be out of sync with the state of the dataset version (deprecation)"
                )
            )
            return Taiga2Client._get_existing_file_and_datafile_type(
                file_format, file_path, feather_extra_path
            )
        else:
            raise TaigaClientConnectionException(
                "ERROR: You are in offline mode and the file you requested is not in the cache."
            )

    def _write_feather_extra(self, path: str, feather_extra: dict):
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

        with open(path, "w+") as f:
            json.dump(feather_extra, f)
            f.close()

    def download_to_cache_for_fetch(
        self,
        datafile_id: str = None,
        dataset_name: str = None,
        dataset_version: Union[str, int] = None,
        datafile_name: str = None,
        force_fetch: bool = False,
        force_convert: bool = False,
        file_format: str = "raw",
        quiet: bool = False,
        encoding: str = None,
    ) -> str:
        if not self._is_connected():
            return self._handle_download_to_cache_for_fetch_unconnected(
                datafile_id,
                dataset_name,
                dataset_version,
                datafile_name,
                force_fetch,
                force_convert,
                file_format,
                quiet,
                encoding,
            )

        data_id, data_name, data_version, data_file = self._validate_file_for_download(
            datafile_id, dataset_name, dataset_version, datafile_name, force_convert
        )

        file_path, temp_path, feather_extra_path = self._get_cache_file_paths(
            data_name, data_version, data_file, file_format
        )

        relative_partial_path = os.path.splitext(os.path.basename(file_path))[0]

        (
            underlying_file_path,
            underlying_temp_path,
            underlying_feather_extra_path,
        ) = self._get_underlying_file_from_cache(relative_partial_path, file_format)
        if underlying_file_path is not None:
            file_path = underlying_file_path
            temp_path = underlying_temp_path
            feather_extra_path = underlying_feather_extra_path

        if force_fetch or force_convert:
            for path in [file_path, temp_path, feather_extra_path]:
                if os.path.exists(path):
                    os.remove(path)

        if os.path.exists(file_path) and (
            file_format == "raw" or os.path.exists(feather_extra_path)
        ):
            return Taiga2Client._get_existing_file_and_datafile_type(
                file_format, file_path, feather_extra_path
            )

        datafile_metadata = self._extract_datafile_metadata(
            data_name, data_version, data_file
        )
        if datafile_metadata["datafile_type"] == "virtual":
            file_path, datafile_type = self.download_to_cache_for_fetch(
                datafile_metadata["underlying_file_id"],
                force_fetch=force_fetch,
                force_convert=force_convert,
                file_format=file_format,
                quiet=quiet,
                encoding=encoding,
            )
            self._add_underlying_file_to_cache(
                relative_partial_path, os.path.splitext(os.path.basename(file_path))[0]
            )
            return file_path, datafile_type

        # If file_format is "raw", download file to cache and return path
        if file_format == "raw":
            self._download_to_local_file(
                data_id,
                data_file,
                force_fetch,
                file_path,
                force_convert,
                file_format,
                quiet,
            )
            return file_path, "Raw"

        datafile_type = self._get_data_file_type(data_name, data_version, data_file)
        feather_extra = {"datafile_type": datafile_type}
        self._write_feather_extra(feather_extra_path, feather_extra)

        # Otherwise, first download the file as a CSV
        self._download_to_local_file(
            data_id,
            data_file,
            force_fetch,
            temp_path,
            force_convert,
            "csv" if datafile_type == "Columnar" else "hdf5",
            quiet,
        )

        # Then read that csv into a Pandas DataFrame and write that to a feather file
        if datafile_type == "Columnar":
            df = pandas.read_csv(temp_path, encoding=encoding)
        else:
            df = read_hdf5(temp_path)

        if datafile_type == "Columnar":
            Taiga2Client.convert_column_types(df)
        df.to_feather(file_path)

        # And finally, delete the temp file
        os.remove(temp_path)

        return file_path, datafile_type

    def get(
        self,
        id: str = None,
        name: str = None,
        version: int = None,
        file: str = None,
        force: bool = False,
        encoding: str = None,
    ) -> pandas.DataFrame:
        """
        Resolve and download (converted if needed) csv file(s). Output a dictionary if no files specified
        :param id: Id of the datasetVersion (not the dataset)
        :param name: Permaname of the dataset. Usually of the form `dataset_name_from_user-xxxx` with xxxx being the 4 characters of a uuid
        :param version: Version of the dataset to get the datafile from
        :param file: Dataset file name
        :param force: Boolean to force kicking off the conversion again from Taiga
        :param encoding: Encoding compatible with the parameter of read_csv of pandas (https://docs.python.org/3/library/codecs.html#standard-encodings)
        :return: Pandas dataframe of the data
        """
        # TODO: Explain the accepted format for encoding
        # We first check if we can convert to a csv
        allowed_conversion_type = self._get_allowed_conversion_type_from_dataset_version(
            id=id, name=name, version=version, file=file
        )
        for conv_type in allowed_conversion_type:
            if conv_type == "raw":
                raise TaigaRawTypeException(
                    "The file is a Raw one, please use instead `download_to_cache` with the same parameters"
                )

        # return a pandas dataframe with the data
        for attempt in range(3):
            try:
                local_file, data_file_type = self.download_to_cache_for_fetch(
                    datafile_id=id,
                    dataset_name=name,
                    dataset_version=version,
                    datafile_name=file,
                    force_convert=force,
                    file_format="feather",
                    encoding=encoding,
                )
            except (
                TaigaDeletedVersionException,
                TaigaClientConnectionException,
                ValueError,
            ) as e:
                print(colorful.red(str(e)))
                return None

            try:
                return Taiga2Client.read_feather_to_df(local_file, data_file_type)
            except Exception as ex:
                print(
                    colorful.red(
                        'Got exception "{}" reading {} from cache. Will remove to force fetch file from Taiga again'.format(
                            ex, local_file
                        )
                    )
                )
                os.unlink(local_file)

        raise Exception("Failed to fetch file multiple times")

    def _get_all_file_names(self, name=None, version=None):
        """Retrieve the name of the files contained in a version of a dataset"""
        assert name is not None, "name has to be set"

        # Get the dataset version ID
        # Get the dataset info and the dataset version info
        # TODO: We are reusing this multiple times (_get_allowed_conversion_type_from_dataset_version) => extract in a function
        id = self._get_dataset_version_id_from_permaname_version(
            name=name, version=version
        )

        api_endpoint = "/api/dataset/{datasetId}/{datasetVersionId}".format(
            datasetId=None, datasetVersionId=id
        )

        result = self.request_get(api_endpoint=api_endpoint)

        # Get the file
        full_dataset_version_datafiles = result["datasetVersion"]["datafiles"]

        return full_dataset_version_datafiles

    def get_all(self, name=None, version=None) -> dict:
        """
        Return all the files from a specific version of a dataset
        :param name:
        :param version:
        :return:
        """
        dict_data_holder = {}

        # TODO: Handle the name containing the version inside
        file_data_s = self._get_all_file_names(name, version)

        # DL each file
        for file_data in file_data_s:
            file_name = file_data["name"]
            try:
                data = self.get(name=name, version=version, file=file_name)
            except TaigaRawTypeException as trte:
                data = self.download_to_cache(
                    name=name, version=version, file=file_name
                )
            dict_data_holder[file_name] = data

        return dict_data_holder

    def is_valid_dataset(
        self,
        id=None,
        name=None,
        version=None,
        file=None,
        force=False,
        format="metadata",
    ):
        try:
            self._get_data_file_json(id, name, version, file, force, format)
            return True
        # If one wants to be more precise, Taiga404Exception is also available
        # TODO: Currently Taiga returns a 500 when receiving a wrong format for datafile. We should change this to not confuse errors meaning
        except TaigaHttpException:
            return False

    def get_short_summary(self, id=None, name=None, version=None, file=None):
        """Get the short summary of a datafile, given the the id/file or name/version/file"""
        if id:
            assert file is not None, "Dataset id must be provided with a specific file"
        if name:
            assert (
                file is not None
            ), "Dataset permaname must be provided with a specific file"
        assert (
            id or name is not None
        ), "Either id or name should be provided, with the corresponding params"

        short_summary = self._get_data_file_summary(
            id=id, name=name, version=version, file=file
        )

        return short_summary

    # <editor-fold desc="Upload">

    def upload_session_files(self, upload_file_path_dict, add_taiga_ids):
        # We first create a new session, and fetch its id
        new_session_api_endpoint = "/api/upload_session"

        new_session_id = self.request_get(
            api_endpoint=new_session_api_endpoint, params=None
        )

        # We upload the files to S3 following the frontend way of doing it, in Taiga Web
        s3_credentials_endpoint = "/api/credentials_s3"
        s3_credentials = self.request_get(api_endpoint=s3_credentials_endpoint)
        # print(s3_credentials)

        bucket = s3_credentials["bucket"]
        partial_prefix = s3_credentials["prefix"]
        full_prefix = partial_prefix + new_session_id + "/"

        # Configuration of the Boto3 client
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=s3_credentials["accessKeyId"],
            aws_secret_access_key=s3_credentials["secretAccessKey"],
            aws_session_token=s3_credentials["sessionToken"],
        )

        # We add every temp_datafile in the session
        new_datafile_api_endpoint = "/api/datafile/" + new_session_id

        for alias, taiga_id in add_taiga_ids:
            data_create_upload_session_file = {
                "filename": alias,
                "filetype": "virtual",
                "existingTaigaId": taiga_id,
            }

            # we don't need to poll the status. it was successful as long as we don't get an exception thrown
            self.request_post(
                api_endpoint=new_datafile_api_endpoint,
                data=data_create_upload_session_file,
            )

        for upload_file_path, format in upload_file_path_dict.items():
            upload_file_object = UploadFile(
                prefix=full_prefix, file_path=upload_file_path, format=format
            )
            print("Uploading {}...".format(upload_file_object.file_name))

            s3_client.upload_file(
                upload_file_path, bucket, upload_file_object.prefix_and_file_name
            )

            S3UploadedData = s3_client.get_object(
                Bucket=bucket, Key=upload_file_object.prefix_and_file_name
            )

            # We now organize the conversion and the reupload
            data_create_upload_session_file = {
                "filename": upload_file_object.file_name,
                "filetype": "s3",
                "s3Upload": {
                    "format": upload_file_object.format.name,
                    "bucket": str(bucket),
                    "key": str(upload_file_object.prefix_and_file_name),
                },
            }

            current_task_id = self.request_post(
                api_endpoint=new_datafile_api_endpoint,
                data=data_create_upload_session_file,
            )

            task_status_api_endpoint = "/api/task_status/" + current_task_id

            print("Conversion and upload...:")
            task_status = self.request_get(api_endpoint=task_status_api_endpoint)

            while (
                task_status["state"] != "SUCCESS" and task_status["state"] != "FAILURE"
            ):
                print("\t {}".format(task_status["message"]))
                time.sleep(1)
                task_status = self.request_get(api_endpoint=task_status_api_endpoint)

            if task_status["state"] == "SUCCESS":
                print(
                    "\n\t Done: {} properly converted and uploaded".format(
                        upload_file_object.file_name
                    )
                )
            else:
                print(
                    "\n\t While processing {}, we got this error {}".format(
                        upload_file_object.file_name, task_status.message
                    )
                )

        return new_session_id

    # TODO: Add the creation of a folder, given a path relative to home ('~')
    def create_dataset(
        self,
        dataset_name: str = None,
        dataset_description: str = None,
        upload_file_path_dict: Dict[str, str] = {},
        add_taiga_ids: List[Tuple[str, str]] = [],
        folder_id: str = None,
    ) -> str:
        """Create a new dataset in Taiga, by default in the Public folder.

        Files can be local files (specified in upload_file_path_dict) or virtual files
        (specified in add_taiga_ids).

        :param dataset_name: 
        :param dataset_description: Description for the new dataset
        :param upload_file_path_dict: Key is the file_path, value is the format
        :param add_taiga_ids: List of tuples where the first item is the name of the
                              datafile in the new version and the second item is the
                              Taiga ID in the format "dataset_permaname.version/file"
        :param folder_id: Folder to place this dataset in.
        :returns: Dataset ID of the new dataset
        """
        assert len(upload_file_path_dict) != 0 or len(add_taiga_ids) != 0
        if folder_id is None:
            folder_id = "public"
            prompt = "Warning: Your dataset will be created in Public. Are you sure? y/n (otherwise use folder_id parameter) "
            try:
                user_continue = raw_input(prompt)
            except NameError:
                user_continue = input(prompt)

            if user_continue != "y":
                return

        new_session_id = self.upload_session_files(
            upload_file_path_dict=upload_file_path_dict, add_taiga_ids=add_taiga_ids
        )

        # We create the dataset since all files have been uploaded
        create_dataset_api_endpoint = "/api/dataset"
        # TODO: Get user id from token
        data_create_dataset = {
            "sessionId": new_session_id,
            "datasetName": dataset_name,
            "currentFolderId": folder_id,
            "datasetDescription": dataset_description,
        }
        dataset_id = self.request_post(
            api_endpoint=create_dataset_api_endpoint, data=data_create_dataset
        )
        print(
            "\nCongratulations! Your dataset `{}` has been created in the {} folder with the id {}. You can directly access to it with this url: {}\n".format(
                dataset_name, folder_id, dataset_id, self.url + "/dataset/" + dataset_id
            )
        )

        return dataset_id

    def update_dataset(
        self,
        dataset_id: str = None,
        dataset_permaname: str = None,
        dataset_version: str = None,
        dataset_description: str = None,
        changes_description: str = None,
        upload_file_path_dict: Dict[str, str] = {},
        add_taiga_ids: List[Tuple[str, str]] = [],
        add_all_existing_files: bool = False,
    ) -> str:
        """Create a new version of a dataset.

        If dataset_version is not provided, the latest dataset version is used.

        Files can be local files (specified in upload_file_path_dict) or virtual files
        (specified in add_taiga_ids).

        :param dataset_id: ID of dataset. Ignored if dataset_permaname is provided
        :param dataset_permaname: Permaname of a dataset. Will retrieve latest dataset
                                  version if no dataset_version provided
        :param dataset_version: Version of a dataset. Ignored if dataset_permaname not
                                provided
        :param dataset_description: Description for the new version (if not provided,
                                    will use existing description)
        :param changes_description: Description of changes for this version
        :param upload_file_path_dict: Key is the file_path, value is the format
        :param add_taiga_ids: List of tuples where the first item is the name of the
                              datafile in the new version and the second item is the
                              Taiga ID in the format "dataset_permaname.version/file"
        :param add_all_existing_files: If True, add all files in the specified dataset
                                       to the new dataset as virtual datafiles, if
                                       name is not already specified in
                                       upload_file_path_dict or add_taiga_ids

        :returns: Dataset version ID of the new version
        """
        assert (not dataset_id and dataset_permaname) or (
            dataset_id and not dataset_permaname and not dataset_version
        )
        assert len(upload_file_path_dict) != 0 or len(add_taiga_ids) != 0

        dataset_json = None
        dataset_version_json = None

        if dataset_permaname and dataset_version:
            # We retrieve the dataset version given
            get_dataset_with_permaname_and_version_api_endpoint = (
                "/api/dataset/" + dataset_permaname + "/" + dataset_version
            )
            result = self.request_get(
                api_endpoint=get_dataset_with_permaname_and_version_api_endpoint
            )
            dataset_json = result["dataset"]
            dataset_version_json = result["datasetVersion"]

            dataset_id = dataset_json["id"]
        elif dataset_permaname and not dataset_version:
            # We retrieve the latest dataset version
            get_latest_dataset_version_id_api_endpoint = (
                "/api/dataset/" + dataset_permaname
            )
            result = self.request_get(
                api_endpoint=get_latest_dataset_version_id_api_endpoint
            )
            latest_dataset_version_metadata = max(
                result["versions"], key=lambda x: int(x["name"])
            )
            dataset_version = latest_dataset_version_metadata["name"]
            if latest_dataset_version_metadata["state"] != "approved":
                print(
                    colorful.orange(
                        "WARNING: The latest version of this dataset is deprecated."
                    )
                )

            get_latest_dataset_version_api_endpoint = (
                "/api/dataset/" + dataset_permaname + "/" + dataset_version
            )
            result = self.request_get(
                api_endpoint=get_latest_dataset_version_api_endpoint
            )
            dataset_json = result["dataset"]
            dataset_version_json = result["datasetVersion"]
            dataset_id = dataset_json["id"]
        else:
            get_dataset_with_id_api_endpoint = "/api/dataset/" + dataset_id + "/last"
            result = self.request_get(api_endpoint=get_dataset_with_id_api_endpoint)
            dataset_version_json = result
            dataset_permaname = self.request_get(
                api_endpoint="/api/dataset/" + dataset_id
            )["permanames"][-1]
            dataset_version = dataset_version_json["version"]

        datafiles = dataset_version_json["datafiles"]

        if add_all_existing_files:
            skip_files = set(
                UploadFile(prefix="", file_path=key, format=file_format).file_name
                for file_path, file_format in upload_file_path_dict.items()
            ).union(set(alias for alias, _ in add_taiga_ids))
            for datafile in datafiles:
                if datafile["name"] not in skip_files:
                    add_taiga_ids.append(
                        (
                            datafile["name"],
                            "{}.{}/{}".format(
                                dataset_permaname, dataset_version, datafile["name"]
                            ),
                        )
                    )

        new_session_id = self.upload_session_files(
            upload_file_path_dict=upload_file_path_dict, add_taiga_ids=add_taiga_ids
        )

        new_dataset_version_params = dict()
        new_dataset_version_params["sessionId"] = new_session_id
        new_dataset_version_params["datasetId"] = dataset_id

        if dataset_description:
            new_dataset_version_params["newDescription"] = dataset_description
        else:
            new_dataset_version_params["newDescription"] = dataset_version_json[
                "description"
            ]

        if changes_description is not None:
            new_dataset_version_params["changesDescription"] = changes_description

        new_dataset_version_api_endpoint = "/api/datasetVersion"

        new_dataset_version_id = self.request_post(
            api_endpoint=new_dataset_version_api_endpoint,
            data=new_dataset_version_params,
        )

        print(
            "\nDataset version with id {} created. You can access to this dataset version directly with this url: {}".format(
                new_dataset_version_id,
                self.url + "/dataset_version/" + new_dataset_version_id,
            )
        )

        return new_dataset_version_id

    # </editor-fold>

    # <editor-fold desc="Utilities">
    def request_get(self, api_endpoint, params=None):
        url = self.url + api_endpoint
        r = requests.get(
            url,
            stream=True,
            params=params,
            headers=dict(Authorization="Bearer " + self.token),
        )

        if r.status_code == 404:
            raise Taiga404Exception(
                "Received a not found error. Are you sure about your credentials and/or the data parameters? params: {}".format(
                    params
                )
            )
        elif r.status_code != 200:
            raise TaigaHttpException("Bad status code: {}".format(r.status_code))

        return r.json()

    def request_post(self, api_endpoint, data, standard_reponse_handling=True):
        assert data is not None

        print("hitting", self.url + api_endpoint)
        r = requests.post(
            self.url + api_endpoint,
            json=data,
            headers=dict(Authorization="Bearer " + self.token),
        )

        if standard_reponse_handling:
            if r.status_code == 404:
                return None
            elif r.status_code != 200:
                raise Exception("Bad status code: {}".format(r.status_code))

            return r.json()
        else:
            return r

    @staticmethod
    def convert_column_types(df: pandas.DataFrame):
        for c in df.columns:
            if df[c].dtype == numpy.object:
                if pandas.api.types.infer_dtype(df[c], skipna=True) in [
                    "integer",
                    "floating",
                    "string",
                    "boolean",
                ]:
                    continue
                df[c] = df[c].apply(lambda v: v if pandas.isnull(v) else str(v))

        # </editor-fold>


TaigaClient = Taiga2Client

try:
    default_tc = TaigaClient()
except Exception as e:
    print("default_tc could not be set for this reason: {}".format(e))
    print(
        "You can import TaigaClient and add your custom options if you would want to customize it to your settings"
    )
