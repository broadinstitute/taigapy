# Taigapy Documentation

## Table of Contents
- [taigapy.TaigaClient](#taigapytaigaclient)
- [taigapy.TaigaClient.get](#taigapytaigaclient_get)
- [taigapy.TaigaClient.download_to_cache](#taigapytaigaclientdownload_to_cache)
- [taigapy.TaigaClient.create_dataset](#taigapytaigaclientcreate_dataset)
- [taigapy.TaigaClient.update_dataset](#taigapytaigaclientupdate_dataset)
- [taigapy.TaigaClient.get_dataset_metadata](#taigapytaigaclientget_dataset_metadata)
- [taigapy.TaigaClient.get_canonical_id](#taigapytaigaclientget_canonical_id)
- [taigapy.TaigaClient.upload_to_gcs](#taigapytaigaclientupload_to_gcs)

## taigapy.TaigaClient
```python
taigapy.TaigaClient(
    self,
    url=DEFAULT_TAIGA_URL, # https://cds.team/taiga
    cache_dir=DEFAULT_CACHE_DIR, # ~/.taiga
    token_path=None
)
```
Client used to interact with [Taiga](https://github.com/broadinstitute/taiga). Keeps a cache of previously retrieved files.
### Parameters
- `url`: _str_\
    The URL of Taiga
- `cache_dir`: _str_\
    Directory where Taiga's cache will live
- `token_path`: _str_\
    Path to Taiga token. If not specified, will first check `./.taiga-token` then `CACHE_DIR/.taiga-token`.
- `figshare_map_file`: _str_\
    Path to Figshare map file, a JSON file with Taiga IDs as keys, and objects of `download_url`, `format`, `column_types`, `encoding` as keys. If specified, will only download files specified in the map, and will download files directly from Figshare.

[Top](#taigapy-documentation)

## taigapy.TaigaClient.get
```python
taigapy.TaigaClient.get(
    id=None,
    name=None,
    version=None,
    file=None,
)
```
Retrieves a Table or NumericMatrix datafile from Taiga (or local cache, if available) and returns it as a `pandas.DataFrame`.

Stores the file in the cache if it is not already stored.

Errors if the requested datafile is not a Table or NumericMatrix (i.e. is a Raw datafile).

If used while offline, will get datafiles that are already in the cache.
### Parameters
- `id`: _str_\
    Datafile ID of the datafile to get, in the form `dataset_permaname.dataset_version/datafile_name`, or `dataset_permaname.dataset_version` if there is only one file in the dataset. Required if `dataset_name` is not provided. Takes precedence if both are provided.
- `name`: _str_\
    Permaname or id of the dataset with the datafile. Required if `id` is not provided. Not used if both are provided.
- `version`: _str_ or _int_\
    Version of the dataset. If not provided, will use the latest approved (i.e. not deprecated or deleted) dataset. Required if `id` is not provided. Not used if both are provided.
- `file`: _str_\
    Name of the datafile in the dataset. Required if `id` is not provided and the dataset contains more than one file. Not used if `id` is provided.
### Returns
`pandas.DataFrame`\
If the file is a NumericMatrix, the row headers will be used as the DataFrame's index.

### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

# The following examples are equivalent, and fetch the file named 'data' from
# the dataset 'Achilles v2.4.6', whose permaname is 'achilles-v2-4-6' and whose
# current version is version 4.

df = tc.get("achilles-v2-4-6.4/data")
df = tc.get("achilles-v2-4-6.4")
df = tc.get(id="achilles-v2-4-6.4/data")
df = tc.get(id="achilles-v2-4-6.4")
df = tc.get(
    name="achilles-v2-4-6",
    version="4",
    file="data"
)
df = tc.get(name="achilles-v2-4-6", version="4")
df = tc.get(
    name="achilles-v2-4-6",
    version=4,
    file="data"
)
df = tc.get(name="achilles-v2-4-6", version=4)
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.download_to_cache
```python
taigapy.TaigaClient.download_to_cache(
    id=None,
    name=None,
    version=None,
    file=None,
)
```
Retrieves a datafile from Taiga in its raw format (CSV or plain text file).

### Parameters
`id`, `name`, `version`, `file` as [taigapy.TaigaClient.get](##taigapy.TaigaClient.get).

### Returns
`str`\
The path of the downloaded file.

### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

raw_path = tc.download("achilles-v2-4-6.4/data")

tc.download("achilles-v2-4-6.4/data", file_path="./data.csv")
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.create_dataset
```python
taigapy.TaigaClient.create_dataset(
    dataset_name,
    dataset_description=None,
    upload_files=None,
    add_taiga_ids=None,
    folder_id=None,
)
```
Creates a new dataset named `dataset_name` with local files `upload_files` and virtual datafiles `add_taiga_ids` in the folder with id `parent_folder_id`.

If multiple files in the union of `upload_files` and `add_taiga_ids` share the same name, Taiga will throw and error and the dataset will not be created.

### Parameters
- `dataset_name`: _str_\
    The name of the new dataset.
- `dataset_description`: _str_\
    Description of the dataset.
- `upload_files`: _list[dict[str, ...]]_\
    List of files to upload, where files are provided as dictionary objects `d` where
    - `d["path"]` is the path of the file to upload
    - `d["name"]` is what the file should be named in the dataset. Uses the base name of the file if not provided
    - `d["format"]` is the [Format](./Definitions.md#DataFile_Formats) of the file (as a string).\
    And optionally,
    - `d["encoding"]` is the character encoding of the file. Uses "UTF-8" if not provided
- `add_taiga_ids`: _list[dict[str, str]]_\
    List of virtual datafiles to add, where files are provided as dictionary objects with keys
    - `"taiga_id"` equal to the Taiga ID of the reference datafile in `dataset_permaname.dataset_version/datafile_name` format
    - `"name"` (optional) for what the virtual datafile should be called in the new dataset (will use the reference datafile name if not provided).
- `folder_id`: _str_\
    The ID of the containing folder. If not specified, will use home folder of user.
### Returns
`str`\
The id of the new dataset.

### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

dataset_id = tc.create_dataset(
    "My cool new dataset",
    dataset_description="The best data",
    upload_files=[
        {
            "path": "~/data_final_final.csv",
            "name": "data",
            "format": "TableCSV",
            "encoding": "ascii",
        }
    ],
    add_taiga_ids=[("achilles-v2-4-6.4/data", "achilles_data")]
)
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.update_dataset
```python
dataset_version_id = taigapy.TaigaClient.update_dataset(
    dataset_id=None,
    dataset_permaname=None,
    dataset_version=None,
    dataset_description=None,
    changes_description=None,
    upload_files=None,
    add_taiga_ids=None,
    add_all_existing_files=True,
)
```
Creates a new version of dataset specified by `dataset_id` or `dataset_name` (and optionally `dataset_version`).

Follows the same rules as [taigapy.TaigaClient.create_dataset](#taigapytaigaclientcreatedataset). Additionally, if a local file listed in `upload_files` matches content (based on SHA256 and MD5 hashes) and [type](./Definitions.md#DataFile_Type)/[format](./Definitions.md#DataFile_Formats) of a datafile in the base dataset version, the local file will not be uploaded, and instead a virtual file will be created based on the existing datafile.

### Parameters
See [taigapy.TaigaClient.get parameters](#Parameters-1) for description of `dataset_name`.\
See [taigapy.TaigaClient.create_dataset parameters](#Parameters-3) for description of `upload_files`, `add_taiga_ids`.
- `dataset_id`: _str_\
    Generated id or id in the format `dataset_permaname.dataset_version`
- `dataset_permaname`: _str_\
    Permaname of the dataset to update. Must be provided if `dataset_id` is not.
- `dataset_version`: _str_ or _int_\
    Dataset version to base the new version off of. If not specified, will use the latest version.
- `dataset_description`: _str_\
    Description of new dataset version. Uses previous version's description if not specified.
- `changes_description`: _str_\
    Description of changes new to this version.
- `add_all_existing_files`: _bool_\
    Whether to add all files from the base dataset version as virtual datafiles in the new dataset version. If a name collides with one in `upload_files` or `add_taiga_ids`, that file is ignored.
### Returns
`str`\
The id of the new dataset version.

### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

dataset_version_id = tc.update_dataset(
    "my-cool-new-dataset-1234",
    dataset_version=1,
    changes_description="added readme",
    upload_files=[
        {
            "file_path": "~/readme.md",
            "format": "Raw",
        }
    ],
    add_all_existing_files=True
)
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.get_dataset_metadata
```python
taigapy.TaigaClient.get_dataset_metadata(
    dataset_name,
    dataset_version=None,
)
```
Gets metadata about the dataset or dataset version specified by `dataset_name` and `dataset_version`.

### Parameters
See [taigapy.TaigaClient.get parameters](#Parameters-1) for description of `dataset_name`, `dataset_version`.

### Returns
`dict`\
If no version is specified, returns a dict of
```python
{
    "can_edit": bool, # whether user can update dataset
    "can_view": bool, # whether user can view dataset and files
    "description": str, # dataset description
    "folders": list[{ # parent folders
        "id": str, # id of folder
        "name": str, # name of folder
    }],
    "id": str, # id of dataset
    "name": str, # name of dataset
    "permanames": list[str], # list of permanames of dataset
    "versions": list[{ # list of versions for dataset
        "id": str, # dataset version id
        "name": str, # dataset version number, as a string
        "state": "approved" or "deprecated" or "deleted" # dataset version state
    }]
}
```
If a version is specified, returns a dict of 
```python
{
    "dataset": dict, # Dict as specified above
    "datasetVersion": {
        "can_edit": bool,
        "can_view": bool,
        "changes_description": str or None, # description of changes for this version
        "creation_date": str, # date in YYYY-MM-DDTHH:mm:ss.fffffffK format
        "creator": {
            "id": str, # id of creator
            "name": str, # username of creator
        },
        "datafiles": [{
            "allowed_conversion_type": [str], # Legacy, not used
            "datafile_type": "s3" or "virtual" or "gcs",
            "gcs_path": str or None, # Google Cloud Storage path if gcs, None otherwise
            "id": str, # datafile id
            "name": str, # datafile name
            "short_summary": str, # number of rows and columns if Columnar, matrix dimensions and number of NAs if HDF5, file size of Raw
            "type": "Columnar" or "HDF5" or "Raw", # See Type under Description
            "underlying_file_id": str # Taiga ID for the s3 or gcs datafile the virtual datafile points to; Only exists for virtual datafiles
        }],
        "dataset_id": str, # id of dataset
        "description": str, # dataset version description
        "figshare": dict or not included, # if dataset version is linked with a figshare article, metadata about the article and files uploaded, otherwise key will not be included
        "folders": [], # Legacy, not used
        "id": str, # dataset version id
        "name": str, # dataset version number, as a string
        "reason_state": str, # if state is not approved, reason for state
        "state": "approved" or "deprecated" or "deleted", # dataset version state
        "version": str # dataset version number, as a string (same as name)
    }
}
```
### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

dataset_metadata = tc.get_dataset_metadata("achilles-v2-4-6")
dataset_version_metadata = tc.get_dataset_metadata("achilles-v2-4-6.4")
dataset_version_metadata = tc.get_dataset_metadata("achilles-v2-4-6", version=4)
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.get_canonical_id
```python
taigapy.TaigaClient.get_canonical_id(
    queried_taiga_id,
)
```
Gets the [canonical ID](./Definitions.md#Canonical_Taiga_ID) for the datafile specified by `queried_taiga_id`.

### Parameters
- `queried_taiga_id`: _str_\
    Taiga ID in the form `dataset_permaname.dataset_version/datafile_name` or `dataset_permaname.dataset_version`

### Returns
### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

canonical_id = tc.get_canonical_id("achilles-v2-4-6.4/data")
canonical_id = tc.get_canonical_id("achilles-v2-4-6.4")
```

[Top](#taigapy-documentation)

## taigapy.TaigaClient.upload_to_gcs
```python
taigapy.TaigaClient.upload_to_gcs(
    queried_taiga_id,
    dest_gcs_path
)
```
Upload a Taiga datafile to a specified location in Google Cloud Storage.

The service account taiga-892@cds-logging.iam.gserviceaccount.com must have `storage.buckets.get` (Storage Legacy Bucket Reader) and `storage.buckets.create` (Storage Legacy Bucket Writer) access for this request. See the [Google Cloud Storage documentation](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add) for instructions on how to grant access.

### Parameters
- `queried_taiga_id`: _str_\
    Taiga ID in the form `dataset_permaname.dataset_version/datafile_name` or `dataset_permaname.dataset_version`
- `dest_gcs_path`: _stf_\
    Google Storage path to upload to, in the form `bucket/path`

### Returns
`bool`\
Whether the file was successfully uploaded

### Examples
```python
from taigapy import TaigaClient

tc = TaigaClient()

upload_successful = tc.upload_to_gcs("achilles-v2-4-6.4/data", "bucket/path.csv")
```

[Top](#taigapy-documentation)