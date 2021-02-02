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
import default_tc as tc

path = tc.download_to_cache("achilles-v2-4-6.4/data") # path is the local path to the downloaded CSV
```

#### Create dataset
Create a new dataset in folder with id `folder_id`, with local files `upload_files` and virtual files `add_taiga_ids`.
```python
import default_tc as tc

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
    folder_id="folder_id", # optional, will default to your home folder if not provided
)
```

#### Update dataset
Create a new dataset in folder with id `folder_id`, with local files `upload_files` and virtual files `add_taiga_ids`.
```python
import default_tc as tc

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
    add_all_existing_files=True, # If True, will add all files from the base dataset version, except files with the same names as those in upload_files or add_taiga_ids
)
```

#### Get dataset metadata
Get metadata about a dataset or dataset version. See fields returned in [TaigaClient API](docs/TaigaClient%20API.md#returns-4)
```python
import default_tc as tc

metadata = tc.get_dataset_metadata("achilles-v2-4-6.4")
```


### Support
Please [open an issue](https://github.com/broadinstitute/taigapy/issues) if you find a bug, or email yejia@broadinstitute.org for general assistance.

## Development
### Setup
In an environment with Python 3.6, run `sh setup.sh` to set up requirements and git hooks.

Run `python setup.py develop`.  

### Running Tests
The fetch (i.e. `get`, `download_to_cache`, `get_dataset_metadata`, etc.) will run against the production Taiga server. The create and update dataset tests will run against your locally hosted Taiga.

To run the fetch tests, run `pytest`.

To run all the tests, [set up Taiga locally](https://github.com/broadinstitute/taiga#installing), then run `pytest --runlocal`.

### Publishing Taigapy
To create a new version, please update the version number in `taigapy/__init__.py` and `git tag` the commit with that version number. Push the tags to GitHub and create a new release with the tag. Update the [changelog](CHANGELOG.md) with the changes.

Publish a new version of taigapy to pypi by executing `publish_new_taigapy_pypi.sh`, which will do the following:
1. `rm -r dist/`
2. `python setup.py bdist_wheel --universal`
3. `twine upload dist/*`
