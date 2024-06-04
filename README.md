# taigaclient v3 

This branch contains an experimental version of a new taigapy client with a slightly different API. (In particular
uploading now has a different API to support specifying what format files are in)

See the sample below for how to use the new taiga client:
```
from taigapy.client_v3 import UploadedFile, LocalFormat, TaigaReference
from taigapy import create_taiga_client_v3
import time

tc = create_taiga_client_v3()
filename = "sample-100x100.hdf5"
start = time.time()
version = tc.create_dataset("test-client-v3", "test client", files=[
    UploadedFile(filename, local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])
print(f"elapsed: {time.time()-start} seconds")

print("fetching file back down")
start = time.time()
df = tc.get(f"{version.permaname}.{version.version_number}/{filename}")
print(f"elapsed: {time.time()-start} seconds")

print("Updating existing dataset")
# update_dataset() will update add/replace the files listed in `additions`
# and/or remove any file listed in `removals`. When specifying files to add
# you can use `UploadedFile` to upload a local file or `TaigaReference` to
# add a reference to an existing file already in taiga by specifying it's taiga ID.

# The below creates a new dataset version with a single additional file,
# carrying forward all other files in that dataset untouched. If there was
# already a file named `sample2` it'd be replaced with the version uploaded.
version = tc.update_dataset(version.permaname, "add file examples", additions=[
    UploadedFile('sample2', local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])

# the below will create a new version with `sample2` removed
version = tc.update_dataset(version.permaname, "remove file", removals=[
   'sample2' ])

# Alternatively, if you want to specify _all_ the files that should be in
# the new dataset version, you can use `replace_dataset`.
#
# The below will create a new version with _only_ the files listed here. The new
# version will only contain `sample3`. 
version = tc.replace_dataset(version.permaname, "test update", files=[
    UploadedFile('sample3', local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])

```

# taigapy
![Run tests](https://github.com/broadinstitute/taigapy/workflows/Run%20tests/badge.svg)

Python client for fetching datafiles from and creating/updating datasets in [Taiga](https://github.com/broadinstitute/taiga).

See [taigr](https://github.com/broadinstitute/taigr) for the R client.

## Table of Contents
- [Quickstart](#quickstart)
  - [Prerequisites](#prerequisites)
  - [Installing](#installing)
  - [Usage](#usage)
    - [Get datafile as dataframe](#get-datafile-as-dataframe)
    - [Download file](#download-file)
    - [Create dataset](#create-dataset)
    - [Update dataset](#update-dataset)
    - [Get dataset metadata](#get-dataset-metadata)
- [Support](#support)
- [Development](#development)
  - [Setup](#setup)
  - [Running Tests](#running-tests)
  - [Publishing Taigapy](#publishing-taigapy)


## Quickstart

### Prerequisites
First, you need to get your authorization token so the client library can make requests on your behalf. Go to https://cds.team/taiga/token/ and click on the "Copy" button to copy your token. Paste your token in a file at `~/.taiga/token`.

```bash
mkdir ~/.taiga/
echo YOUR_TOKEN_HERE > ~/.taiga/token
```

### Installing
Use the package manager [pip](https://pip.pypa.io/en/stable/) to install taigapy.

```bash
pip install taigapy
```

### Usage
See [docs](docs/) for the complete documentation.

#### Get datafile as dataframe
Get a NumericMatrix/HDF5 or TableCSV/Columnar file from Taiga as a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html)
```python
from taigapy import TaigaClient

tc = TaigaClient() # These two steps could be merged in one with `from taigapy import default_tc as tc`

df = tc.get("achilles-v2-4-6.4/data") # df is a pandas DataFrame, with data from the file 'data' in the version 4 of the dataset 'achilles-v2-4-6'
```

#### Download file
Download the raw (plaintext of Raw, CSV otherwise) file from Taiga
```python
from taigapy import default_tc as tc

path = tc.download_to_cache("achilles-v2-4-6.4/data") # path is the local path to the downloaded CSV
```

#### Create dataset
Create a new dataset in folder with id `folder_id`, with local files `upload_files` and virtual files `add_taiga_ids`.
```python
from taigapy import default_tc as tc

new_dataset_id = tc.create_dataset(
    "dataset_name",
    dataset_description="description", # optional (but recommended)
    upload_files=[
        {
            "path": "path/to/file",
            "name": "name of file in dataset", # optional, will use file name if not provided
            "format": "Raw", # or "NumericMatrixCSV" or "TableCSV"
            "encoding": "utf-8" # optional (but recommended), will use iso-8859-1 if not provided
        }
    ],
    add_taiga_ids=[
        {
            "taiga_id": "achilles-v2-4-6.4/data",
            "name": "name in new dataset" # optional, will use name in referenced dataset if not provided (required if there is a name collision)
        }
    ],
    add_gcs_files=[
        {
            "gcs_path": "gs://bucket_name/file_name.extension",
            "name": "name of file in dataset",
        }
    ],
    folder_id="folder_id", # optional, will default to your home folder if not provided
)
```

#### Update dataset
Create a new dataset in folder with id `folder_id`, with local files `upload_files` and virtual files `add_taiga_ids`.
```python
from taigapy import default_tc as tc

new_dataset_id = tc.update_dataset(
    "dataset_permaname",
    changes_description="description",
    upload_files=[
        {
            "path": "path/to/file",
            "name": "name of file in dataset", # optional, will use file name if not provided
            "format": "Raw", # or "NumericMatrixCSV" or "TableCSV"
            "encoding": "utf-8" # optional (but recommended), will use iso-8859-1 if not provided
        }
    ],
    add_taiga_ids=[
        {
            "taiga_id": "achilles-v2-4-6.4/data",
            "name": "name in new dataset" # optional, will use name in referenced dataset if not provided (required if there is a name collision)
        }
    ],
    add_gcs_files=[
        {
            "gcs_path": "gs://bucket_name/file_name.extension",
            "name": "name of file in dataset",
        }
    ],
    add_all_existing_files=True, # If True, will add all files from the base dataset version, except files with the same names as those in upload_files or add_taiga_ids
)
```

#### Get dataset metadata
Get metadata about a dataset or dataset version. See fields returned in [TaigaClient API](docs/TaigaClient%20API.md#returns-4)
```python
from taigapy import default_tc as tc

metadata = tc.get_dataset_metadata("achilles-v2-4-6.4")
```


### Support
Please [open an issue](https://github.com/broadinstitute/taigapy/issues) if you find a bug, or email yejia@broadinstitute.org for general assistance.

## Development
### Setup

Run `poetry install`

Then you can run `poetry shell` to get an environment with the module
installed.


### Running Tests
The fetch (i.e. `get`, `download_to_cache`, `get_dataset_metadata`, etc.) will run against the production Taiga server. The create and update dataset tests will run against your locally hosted Taiga.

To run the fetch tests, run `pytest`.

To run all the tests, [set up Taiga locally](https://github.com/broadinstitute/taiga#installing), then run `pytest --runlocal`.

### Versioning and Publishing Taigapy
### Commit Conventions
We use a tool called [commitizen-tools/commitizen](https://github.com/commitizen-tools/commitizen) for versioning. The way commitizen works is by checking if there are any new commits that follow the formatting rules defined in our `pyproject.toml`'s `bump_pattern` and `bump_map` variables. By default, commitizen uses [conventional commits](https://www.conventionalcommits.org/), however, we have selected a subset of rules to fit most of our current use cases.

In general, when making commits, especially directly to master, please try to adhere to our defined rules so we can ensure versioning is being updated properly:

- fix: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- build: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- chore: COMMIT_MESSAGE -> Correlates with PATCH in SemVer
- feat: COMMIT_MESSAGE -> Correlates with MINOR in SemVer
- fix!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- build!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- chore!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer
- feat!: COMMIT_MESSAGE -> Correlates with MAJOR in SemVer

In addition, we also have `test`, `refactor`, `style`, `docs`, `perf`, `ci` commit types available. While these commit types are not used for determining versioning, they may be helpful in helping organize our commits more.

If these rules are hard to remember, you can also use commitizen's CLI to help format your commits by calling:

    cz c

Instead of

    git commit -m "feat: New feature"

#### Pull requests

Pull request titles with master as target branch should also adhere to our defined rules for commits, especially for squash merges. This is because on Github, we will ultimately use the pull request title as the default commit message.

**NOTE: Our CI/CD pipeline includes a Github actions workflow `run_tests_autobump.yml` that auto-versions and publishes taigapy client. The below instructions are only for if you want to publish locally though this is not recommended!**
Note: this will publish the resulting module to an internal package repo. Before you do this,
you'll need to set yourself up to be able to publish to `python-public`:

To setup for publishing (Based on https://medium.com/google-cloud/python-packages-via-gcps-artifact-registry-ce1714f8e7c1 )

```
poetry self add keyrings.google-artifactregistry-auth
poetry config repositories.public-python https://us-central1-python.pkg.dev/cds-artifacts/public-python/
# also make sure you've authentication via "gcloud auth login" if you haven't already
```

### Installing taigapy from the Google Artifact Registry using poetry

To install taigapy using poetry in your repo run the following:
1. Update your poetry version, and install keyring, and the GCP Artifact Registry backend in the core poetry virtual environment: `poetry self update && poetry self add keyrings.google-artifactregistry-auth`
2. Note that you may need to authenticate with gcloud application by running `gcloud auth application-default login` if you are not already authenticated. 
3. Configure the package source as an explicit package source for your project: `poetry source add --priority=explicit gcp-artifact-registry https://us-central1-python.pkg.dev/cds-artifacts/public-python/simple`
4. Add the python package: `poetry add --source gcp-artifact-registry taigapy`

This should install taigapy from the CDS' internal public-python atrifact registry in your poetry env.
