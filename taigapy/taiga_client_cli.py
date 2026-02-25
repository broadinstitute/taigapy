import argparse
import json
import os

from taigapy import DEFAULT_TAIGA_URL, create_taiga_client_v3, LocalFormat
from taigapy.utils import format_datafile_id
from taigapy.client_v3 import TaigaReference
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


def get_dataset_file_ids(tc, dataset_permaname, version):
    result = []
    files = tc.get_dataset_metadata(dataset_permaname, version)
    for file in files['datasetVersion']['datafiles']:
        if "underlying_file_id" in file:
            canonical_id = file["underlying_file_id"]
        else:
            canonical_id = f"{dataset_permaname}.{version}/{file['name']}"
        result.append((file['name'], canonical_id, file['original_file_sha256']))
    return result

def calc_delta(tc, base_id, last_id, ignore_sha=False):
    def _dict(items):
        result = {}
        for name, canonical_id, sha256 in items:
            result[name] = (canonical_id, sha256)
        return result
    base = _dict(get_dataset_file_ids(tc, *base_id.split(".")))
    last = _dict(get_dataset_file_ids(tc, *last_id.split(".")))
    
    deleted = set(base.keys()).difference(last.keys())
    added = set(last.keys()).difference(base.keys())
    
    if ignore_sha:
        def is_same(old, new):
            return old[0] == new[0]
    else:
        def is_same(old, new):
            return old[1] == new[1]
    
    changed = {k : (base[k][0], last[k][0]) for k in set(base.keys()).intersection(last.keys()) if not is_same(base[k], last[k]) }
    return dict(deleted=deleted, added=added, changed=changed)
        
def diff(args):
    tc = _get_taiga_client(args)
    changes = calc_delta(tc, args.taiga_id_1, args.taiga_id_2)
    if len(changes['added']) > 0:
        print("Added:")
        for name in changes['added']:
            print(f"\t{name}")
        print("")

    if len(changes['changed']) >0:    
        print("Changed:")
        for name, (old_v, new_v) in changes["changed"].items():
            print(f"\t{name}")
            print(f"\t\told: {old_v}")
            print(f"\t\tnew: {new_v}")
        print("")
    
    if len(changes['deleted']) > 0:
        print("Removed:")
        for name in changes['deleted']:
            print(f"\t{name}")
        print("")

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


def copy(args):
    """
    Copy all files from source dataset to a new destination dataset using references
    """
    tc = _get_taiga_client(args)

    if "." in args.source_id:
        source_permaname, version_number = args.source_id.split(".")
    else:
        # if no version specified, look up the latest version
        source_dataset_metadata = tc.get_dataset_metadata(args.source_id)
        if source_dataset_metadata is None:
            print(f"Error: Source dataset {args.source_id} not found")
            return

        source_permaname = args.source_id
        version_number = get_latest_valid_version_from_metadata(source_dataset_metadata)

    # Get detailed metadata for this version
    source_version_metadata = tc.get_dataset_metadata(
        source_permaname, version=version_number
    )
    
    # Extract datafiles from the source dataset
    datafiles = source_version_metadata["datasetVersion"]["datafiles"]
    
    assert datafiles, f"No files found in source dataset {args.source_id}"
    
    reference_files = []
    skipped_files = []
    
    for datafile in datafiles:
        if "type" not in datafile:
            # Skip GCS files as they can't be taiga referenced
            # Checking type as mentioned in client.py get_canonical_id method
            skipped_files.append(datafile["name"])
            continue
        
        taiga_id = source_permaname + "." + version_number + "/" + datafile["name"]

        # Preserve any custom metadata from the original file
        custom_metadata = datafile.get("custom_metadata", {})
        
        reference_files.append(
            TaigaReference(
                name=datafile["name"],
                taiga_id=taiga_id,
                custom_metadata=custom_metadata
            )
        )
    
    if skipped_files:
        print(f"Warning: Skipped {len(skipped_files)} files that couldn't be referenced: {', '.join(skipped_files)}")
    
    assert reference_files, "No files could be referenced. Copy operation aborted."

    if args.dryrun:
        action = "update" if args.update else "create"
        print(f"Dry run: Would {action} dataset '{args.destination_name}' with {len(reference_files)} referenced files:")
        for ref in reference_files:
            print(f"  - {ref.name} -> {ref.taiga_id}")
        return

    if args.update:
        print(f"Updating dataset '{args.destination_name}' with {len(reference_files)} referenced files")
        result = tc.update_dataset(
            args.destination_name,
            reason=f"Copy of {args.source_id} via taigaclient copy command",
            additions=reference_files
        )
    else:
        print(f"Creating new dataset '{args.destination_name}' with {len(reference_files)} referenced files")
        result = tc.create_dataset(
            args.destination_name,
            description=f"Copy of {args.source_id} created via taigaclient copy command",
            files=reference_files
        )

    print(f"Successfully {'updated' if args.update else 'created'} dataset: {args.destination_name}, permaname: {result.permaname}")
    

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

    parser_diff = subparsers.add_parser("diff", help="Compare two taiga datasets")
    parser_diff.add_argument("taiga_id_1")
    parser_diff.add_argument("taiga_id_2")
    parser_diff.set_defaults(func=diff)

    # copy command parser
    parser_copy = subparsers.add_parser(
        "copy", help="Copy files from a source dataset to a new destination dataset using taiga references"
    )
    parser_copy.add_argument(
        "source_id", help="Source dataset permaname"
    )
    parser_copy.add_argument(
        "destination_name", help="Name for the new dataset"
    )
    parser_copy.add_argument(
        "--dryrun", action="store_true", help="Show what would be copied without creating the dataset"
    )
    parser_copy.add_argument(
        "--update", action="store_true", help="Update an existing dataset (destination_name is treated as a permaname)"
    )
    parser_copy.set_defaults(func=copy)

    args = parser.parse_args()

    args.func(args)

if __name__ == "__main__":
    main()