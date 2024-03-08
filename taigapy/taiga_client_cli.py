import argparse
import json
import os

from taigapy import DEFAULT_TAIGA_URL, create_taiga_client_v3, LocalFormat
from taigapy.utils import format_datafile_id
from typing import Optional
from .types import DatasetVersionState
from .custom_exceptions import TaigaDeletedVersionException
from taigapy.utils import get_latest_valid_version_from_metadata
import colorful as cf

def _get_taiga_client(args: argparse.Namespace):
    """Get TaigaClient based on args from either `fetch` or `dataset_meta`"""
    url = DEFAULT_TAIGA_URL
    cache_dir = None
    if args.taiga_url is not None:
        url = args.taiga_url
    if args.data_dir is not None:
        cache_dir = os.path.expanduser(args.data_dir)

    return create_taiga_client_v3(url=url, cache_dir=cache_dir)


def _validate_file_for_download(
    client,
    id_or_permaname: Optional[str],
    dataset_name: Optional[str],
    dataset_version: Optional[str],
    datafile_name: Optional[str],
):
    if id_or_permaname is None and dataset_name is None:
        # TODO standardize exceptions
        raise ValueError("id or name must be specified")
    elif (
        id_or_permaname is None
        and dataset_name is not None
        and dataset_version is None
    ):
        dataset_metadata = (
            client.api.get_dataset_version_metadata(dataset_name, None)
        )
        dataset_version = get_latest_valid_version_from_metadata(dataset_metadata)
        print(
            cf.orange(
                "No dataset version provided. Using version {}.".format(
                    dataset_version
                )
            )
        )

    metadata = client.api.get_datafile_metadata(
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
        raise TaigaDeletedVersionException(
            "{} version {} is deleted. The data is not available anymore. Contact the maintainer of the dataset.".format(
                dataset_permaname, dataset_version
            )
        )

    return metadata

from .custom_exceptions import Taiga404Exception
import traceback

def _get_datafile_params(args):
    if args.requestjson:
        with open(args.requestjson, "rt") as fd:
            params = json.load(fd)
            data_file_id = params["data_file_id"]
            name = params["name"]
            version= params["version"]
            file = params["file"]
    else:
        data_file_id = args.data_file_id
        name = args.name
        version = args.version
        file = args.file
    return data_file_id, name, version, file

def fetch(args):
    if args.data_file_id is None and args.name is None and args.requestjson is None:
        raise Exception("data_file_id or name or requestjson must be set")

    tc = _get_taiga_client(args)

    data_file_id, name, version, file = _get_datafile_params(args)

    schema_version = "1"

    try:
        datafile_metadata = _validate_file_for_download(tc,
            data_file_id, name, version, file
        )
        datafile_id = format_datafile_id( 
            datafile_metadata.dataset_permaname,
            datafile_metadata.dataset_version,
            datafile_metadata.datafile_name,
        )

        if args.format == "raw":
            requested_format = LocalFormat.RAW
        else:
            assert args.format == "feather"
            # determine whether this is a table or a matrix
            allowed_formats = tc.get_allowed_local_formats(datafile_id)
            if LocalFormat.FEATHER_TABLE in allowed_formats:
                requested_format = LocalFormat.FEATHER_TABLE
            else:
                assert LocalFormat.FEATHER_MATRIX in allowed_formats, f"allowed formats were: {allowed_formats} but looking for FEATHER_MATRIX"
                requested_format = LocalFormat.FEATHER_MATRIX

        datafile = tc.download_to_cache(datafile_id, requested_format=requested_format)
        d = {
            "schema_version": schema_version,
            "filename": datafile,
            "datafile_type": requested_format.value,
            "error": False,
        }
    except Taiga404Exception:
        # no data found
        d = {
            "schema_version": schema_version,
            "error": True,
            "message": "Not found"
        }
    except Exception:
        print( print(traceback.format_exc()))
        d = {
            "schema_version": schema_version,
            "error": True,
        }

    if args.write_filename is not None:
        with open(args.write_filename, "wt") as f:
            json.dump(d, f)
    else:
        print(d)


def dataset_meta(args):
    tc = _get_taiga_client(args)
    metadata = tc.get_dataset_metadata(
        args.dataset_name, version=args.version, version_id=args.version_id
    )
    if args.write_filename is not None:
        with open(args.write_filename, "w+") as f:
            j = json.dump(metadata, f)
            f.close()
    else:
        print(metadata)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--taiga-url", help="Override default Taiga url (https://cds.team/taiga)"
    )

    parser.add_argument(
        "--data-dir", help="Path to where token file lives and cached downloaded files"
    )

    subparsers = parser.add_subparsers(title="commands", dest="command")

    # fetch command parser
    parser_fetch = subparsers.add_parser(
        "fetch", help="Download a Taiga file into the cache directory"
    )
    parser_fetch.add_argument(
        "data_file_id",
        nargs="?",
        default=None,
        help="Taiga ID or datafile ID. If not set, NAME must be set",
    )
    parser_fetch.add_argument(
        '--requestjson', help="Path to a json file containing data_file_id, version, file, name instead of passing them on the command line to avoid issues with escaping parameters" 
    )
    parser_fetch.add_argument(
        "--name", help="Dataset name. Must be set if data_file_id is not set."
    )
    parser_fetch.add_argument("--version", help="Dataset version")
    parser_fetch.add_argument("--file", help="Datafile name")
    parser_fetch.add_argument(
        "--format",
        choices=["raw", "feather"],
        default="feather",
        help="Format to store file. If Taiga file is a raw file, choose raw. Otherwise, the default is feather.",
    )
    parser_fetch.add_argument(
        "--write-filename",
        help="If set, will write the full path and Taiga file type of the cached file to WRITE_FILENAME. Otherwise, will write to stdout",
    )
    parser_fetch.set_defaults(func=fetch)

    # dataset-meta command parser
    parser_dataset_meta = subparsers.add_parser(
        "dataset-meta",
        help="Fetch the metadata about a dataset (or dataset version if version-id is provided).",
    )
    parser_dataset_meta.add_argument(
        "dataset_name", nargs="?", default=None, help="Dataset name or ID"
    )
    parser_dataset_meta.add_argument("--version", help="Dataset version")
    parser_dataset_meta.add_argument("--version-id", help="Dataset version ID")
    parser_dataset_meta.add_argument(
        "--write-filename",
        help="If set, will write the metadata to WRITE_FILENAME. Otherwise, will write to stdout",
    )
    parser_dataset_meta.set_defaults(func=dataset_meta)

    args = parser.parse_args()

    args.func(args)

if __name__ == "__main__":
    main()