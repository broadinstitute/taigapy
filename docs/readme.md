## Key Concepts

### Dataset Structure
- **Dataset**: A versioned collection of related data files. Any addition or removal creates a new version of the dataset.
- **Version**: A specific snapshot of a dataset
- **Permaname**: A unique identifier for a dataset that remains constant across versions
- **Datafile**: An individual file within a dataset version

### File Formats
The client supports several file formats:

- `HDF5_MATRIX`: HDF5 format for matrix data
- `PARQUET_TABLE`: Parquet format for tabular data
- `CSV_TABLE`: CSV format for tabular data
- `CSV_MATRIX`: CSV format for matrix data
- `RAW`: Raw binary data
- `FEATHER_TABLE`: Feather format for tabular data
- `FEATHER_MATRIX`: Feather format for matrix data

## Basic Usage

### Retrieving Data

```python
from taigapy import create_taiga_client_v3

client = create_taiga_client_v3()

# Using a complete Taiga ID (perferred)
df = client.get(id="dataset-name.1/file.csv")

# Or fetch data specifying dataset name, version, and file
df = client.get(name="dataset_name", version=1, file="file.csv")

```

### Creating a New Dataset

```python
files = [
    UploadedFile(
        name="data",
        local_path="/path/to/data.csv",
        format=LocalFormat.CSV_TABLE
    )
]

dataset = client.create_dataset(
    name="My Dataset",
    description="Description of the dataset",
    files=files
)
```

### Updating an Existing Dataset

```python
# Add new files to an existing dataset. Files with the 
# same name as an existing file will replace it in the new 
# dataset version. All other files are retained in the new
# dataset version unaltered.

new_files = [
    UploadedFile(
        name="new_data",
        local_path="/path/to/new_data.csv",
        format=LocalFormat.CSV_TABLE
    )
]

updated_dataset = client.update_dataset(
    permaname="dataset-permaname",
    reason="Adding new data file",
    additions=new_files
)
```

### Replacing Dataset Content

```python
# Replace all files in an existing dataset. Any files not listed in
# replacement_files will no longer be part of the dataset.

replacement_files = [
    UploadedFile(
        name="updated_data.csv",
        local_path="/path/to/updated_data.csv",
        format=LocalFormat.CSV_TABLE
    )
]

new_version = client.replace_dataset(
    permaname="dataset-permaname",
    reason="Updating with new data",
    files=replacement_files
)
```

## File Types

When specifying files to add to a dataset, one can specify either an instance of `UploadedFile`, if you want to upload a local file, or `TaigaReference` if you want to
add a file which is already in Taiga.

### UploadedFile
For uploading a file from the local filesystem:
```python
UploadedFile(
    name="filename.csv",          # Name in Taiga
    local_path="/path/to/file",   # Local file path
    format=LocalFormat.CSV_TABLE, # File format. See "File Formats" above for all options.
    encoding="utf8",              # Optional encoding
    custom_metadata={}            # Optional metadata
)
```

### TaigaReference
For referencing existing files in Taiga:
```python
TaigaReference(
    name="new_name.csv",     # New name for the reference
    taiga_id="existing.id",  # ID of existing Taiga file
    custom_metadata={}       # Optional metadata
)
```

## Advanced Features

### Downloading to Specific Formats
```python
# Download and convert to a specific format
local_path = client.download_to_cache(
    "dataset.1/file.csv",
    requested_format=LocalFormat.PARQUET_TABLE
)
```

Since not all files can be converted to all formats, you can use `get_allowed_local_formats()` to find out which formats are allowed for a given file.

### Getting Dataset Metadata
```python
# Get metadata for a specific dataset version
metadata = client.get_dataset_metadata(
    permaname="dataset-name",
    version="1"
)
```

### Uploading to Google Cloud Storage
(deprecated)
```python
success = client.upload_to_gcs(
    data_file_taiga_id="dataset.1/file.csv",
    requested_format=LocalFormat.CSV_TABLE,
    dest_gcs_path_for_file="gs://bucket-name/path/to/file.csv"
)
```

