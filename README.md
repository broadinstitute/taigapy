# taigapy
Library for reading from taiga in python

See [here](https://confluence.broadinstitute.org/display/CPDS/Taiga) for installing taigr, the library for reading from taiga in R

## Token set up

First, you need to get your authorization token so the client library can make requests on your behalf. Go to https://cds.team/taiga/token/ and click on the "Copy" button to copy your token. Paste your token in a file at `~/.taiga/token`.

```
mkdir ~/.taiga/
echo YOUR_TOKEN_HERE > ~/.taiga/token
```

## Installing Taigapy

If you are only using Taigapy and not making modifications to it, run

```
pip install taigapy
```

If you are developing Taigapy, check out the repo and run

```
python setup.py develop
```

## Usage

### Main methods

#### Download

- If you want a file from a dataset to use within Python, use `.get` method (this will save a file in your cache in [feather format](https://github.com/wesm/feather))
- If you want all the files from a specific dataset to use within Python, use `.get_all` method
- If you need a Raw file (for example, plaintext files) or you want to save a file in a specific format, use the `.download_to_cache` method

Examples:

```python
from taigapy import TaigaClient

tc = TaigaClient() # These two steps could be merged in one with `from taigapy import default_tc as tc`

# The following 6 examples are equivalent, and fetch the file named 'data' from the
# dataset 'Achilles v2.4.6', whose permaname is 'achilles-v2-4-6' and whose current
# version is version 4.

# 1. Fetch a file using dataset ID
df = tc.get(id='022b5ee4df914362945afb8a4dd55d1e')

# 2. Fetch a file using dataset version ID
df = tc.get(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b')

# 3. Fetch a file using dataset permaname and version
df = tc.get(name='achilles-v2-4-6', version=4)

# 4. Specify the file (optionally if it's the only file, required if there are multiple
#    files)
df = tc.get(name='achilles-v2-4-6', version=4, file='data')

# 5. Fetch a file using dataset permaname and version as the id using the format
#    permaname.version
df = tc.get(id='achilles-v2-4-6.4')

# 6. Specify the file (optionally if it's the only file, required if there are multiple
#    files)
df = tc.get(name='achilles-v2-4-6.4/data')

# If you want the file as a CSV, or want a Raw file, download with download_to_cache
# and get the local file path.
# The options for format are
# - csv, gct, hdf5, tsv for Matrix files
# - csv and tsv for Table files
# - raw for Raw files (this is the default)
raw_path = tc.download_to_cache(name='achilles-v2-4-6', version=4, file='data', format='csv')
```

#### Upload

You can also upload data into taiga (see below for available formats). Methods are:
- Create a dataset with `create_dataset`
- Update a dataset with `update_dataset`

Examples:

- Create a new dataset in folder public (you can find the folder_id in the url of Taiga web)

```python
from taigapy import TaigaClient

tc = TaigaClient()

# Create a new dataset in public
dataset_id = tc.create_dataset(
    dataset_name='My Dataset Name',
    dataset_description='My Dataset Description',
    upload_file_path_dict={'file_one_path': 'format'},
    folder_id='public'
)
```

- Update an existing dataset (i.e. create a new dataset version)

```python
from taigapy import TaigaClient

tc = TaigaClient()

# Update a dataset with new files using dataset_id (will use latest version as base)
new_dataset_version_id = tc.update_dataset(
    dataset_id=dataset_id, 
    upload_file_path_dict={'file_updated_or_new_path': 'format'},
)

# Update a dataset using permaname only (will use latest version as base)
new_dataset_version_id = tc.update_dataset(
    dataset_permaname=dataset_permaname,
    upload_file_path_dict={'file_updated_or_new_path': 'format'}
)

# Update a dataset using permaname and version
new_dataset_version_id = tc.update_dataset(
    dataset_permaname=dataset_permaname,
    dataset_version=2,
    upload_file_path_dict={'file_updated_or_new_path': 'format'},
)

# Update a dataset with virtual files (files already on Taiga)
new_dataset_version_id = tc.update_dataset(
    dataset_id=dataset_id,
    add_taiga_ids=['name_in_this_dataset': 'dataset.version/existing_file']
)

# Update a dataset and its description
new_dataset_version_id = tc.update_dataset(
    dataset_id=dataset_id,
    upload_file_path_dict={'file_updated_or_new_path': 'format'},
    description='New description for dataset'
)

# Update a dataset and add a description of changes for this version
new_dataset_version_id = tc.update_dataset(
    dataset_id=dataset_id,
    upload_file_path_dict={'file_updated_or_new_path': 'format'},
    changes_description='Description of changes for this version'
)

# Update a dataset and add all files from the base dataset version as virtual files
new_dataset_version_id = tc.update_dataset(
    dataset_id=dataset_id,
    upload_file_path_dict={'file_new_path': 'format'},
    add_all_existing_files=True
)
```

Formats available for upload are:

- NumericMatrixCSV
- NumericMatrixTSV
- TableCSV
- TableTSV
- GCT
- Raw

### Offline mode

If you are not connected to Taiga but you have a file in your cache, you can get the file from your cache if you call `get` with `id` in the `dataset_permaname.dataset_version/datafile_name` form, or if you provide `name`, and `version`, and `file`.

## Running Taigapy via Command line

Run `python -m taigapy -h` to have an up to date help.

### Create a new dataset

`python -m taigapy create -n dataset_name -f {'file_path_one': 'format', ...}`

### Update an existing dataset 

`python -m taigapy update -p dataset_permaname -v dataset_version -f {'file_path_one': 'format', ...}`

### Get a dataset from Taiga

`python -m taigapy get -p dataset_permaname -v dataset_version -f file_name -t format`

[Important] Please choose a format available for this specific file in taiga Web UI

## `taigaclient`

A command-line script, `taigaclient`, is also available. It is installed with the `taigapy` package, and it currently supports downloading files to the cache via the `fetch` command and getting metadata about a dataset via the `dataset-meta` command.

### Usage
```
usage: taigaclient [-h] [--taiga-url TAIGA_URL] [--data-dir DATA_DIR]
                   {fetch,dataset-meta} ...

optional arguments:
  -h, --help            show this help message and exit
  --taiga-url TAIGA_URL
                        Override default Taiga url (https://cds.team/taiga)
  --data-dir DATA_DIR   Path to where token file lives and cached downloaded
                        files

commands:
  {fetch,dataset-meta}
    fetch               Download a Taiga file into the cache directory
    dataset-meta        Fetch the metadata about a dataset.
```

#### fetch
```
usage: taigaclient fetch [-h] [--name NAME] [--version VERSION] [--file FILE]
                         [--force-fetch] [--quiet] [--force-convert]
                         [--format {raw,feather}]
                         [--write-filename WRITE_FILENAME]
                         [data_file_id]

positional arguments:
  data_file_id          Taiga ID or datafile ID. If not set, NAME must be set

optional arguments:
  -h, --help            show this help message and exit
  --name NAME           Dataset name. Must be set if data_file_id is not set.
  --version VERSION     Dataset version
  --file FILE           Datafile name
  --force-fetch         If set, will bypass local cache and try to redownload
                        from Taiga
  --quiet               If set, do not print progress
  --force-convert       Ask Taiga to convert this file again (Implies --force-
                        fetch)
  --format {raw,feather}
                        Format to store file. If Taiga file is a raw file,
                        choose raw. Otherwise, the default is feather.
  --write-filename WRITE_FILENAME
                        If set, will write the full path and Taiga file type
                        of the cached file to WRITE_FILENAME. Otherwise, will
                        write to stdout
```

#### dataset-meta
```
usage: taigaclient dataset-meta [-h] [--version VERSION]
                                [--write-filename WRITE_FILENAME]
                                dataset_name

positional arguments:
  dataset_name          Dataset name

optional arguments:
  -h, --help            show this help message and exit
  --version VERSION     Dataset version
  --write-filename WRITE_FILENAME
                        Path to JSON file to write metadata
```

## Publish Taigapy on pypi
`pip install twine` (not to be confused with the interactive fiction software called twine)

Execute: `publish_new_taigapy_pypi.sh` which will do the following:

1. `rm -r dist/`
2. `python setup.py bdist_wheel --universal`
3. `twine upload dist/*`

## More Taigapy information:

Confluence: https://confluence.broadinstitute.org/display/CPDS/Taiga

## Running tests:
`pytest`