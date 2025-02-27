# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 3.13.0 (2025-02-27)


- feat: Added a copy feature to taigaclient cli (#27)

## 3.12.0 (2025-01-22)


- feat: forcing package deploy attempt

## 3.11.0 (2025-01-21)


- feat: Added get_latest_version_id method
- Used for looking up the latest version for a given taiga dataset permaname

## 3.10.0 (2025-01-06)


- feat: get_datafile_metadata now returns original_file_sha256
- get_datafile_metadata() returns instances of MinDataFileMetadata which
now have a field named `original_file_sha256`. This hash is the sha256 hash
of the file that was originally uploaded to Taiga and is useful for checking to
see whether a file in Taiga is the same as one you have locally. (For example,
checking before uploading a file. If the file is the same, no need to upload.)

## 3.9.1 (2024-11-20)


- fix: Pull from master to get latest version before publishing

## 3.9.0 (2024-10-21)


- feat: Accumulated changes not yet published to repo

## 3.8.5 (2024-07-25)


- fix: still trying to figure out updating version

## 3.8.4 (2024-07-25)


- fix: Dummy commit in order to bump version
- chore: Removed constraint preventing pandas > 2

## 3.8.3 (2024-04-11)


- chore: Update commitizen hook version
- build: Add commitizen hook pre-merge-commit
- build: Add commitizen
- Test commit for Github Actions

## 3.7.1 (2024-03-07)

## 3.7.0 (2024-02-29)

## [3.3.5] - 2023-04-14
### Added
- Added "custom_metadata" dictionary support to creating, updating, and downloading datasets

## [3.3.4] - 2022-06-09
### Added
- tc.create_dataset, tc.update_dataset, and tc.download_to_cache now support gcs path files
- Data file read access is logged
- Improved parallel uploads

## [3.3.3] - 2022-4-1
### Fixed
- Changed to more lax library requirements

## [3.3.1] - 2021-04-07
### Fixed
- Typo with async upload

## [3.3.0] - 2021-04-07
### Added
- Option to upload synchronously (serially) when creating or updating a dataset

### Fixed
- Fixed bug where getting the DataFrame for a datafile failed after downloading the raw file

## [3.2.4] - 2021-03-22

### Fixed
- Fixed bug where getting the DataFrame for a datafile failed after downloading the raw file

## [3.2.3] - 2021-02-26

### Fixed
- Fixed typos in `taigaclient` that were preventing downloading files

## [3.2.2] - 2021-02-18

### Fixed
- Fixed issue where `TaigaClient.get` cached files under the wrong key when only `name` was supplied

## [3.2.1] - 2021-02-01

### Fixed
- Fixed uploading using Jupyter Notebooks

## [3.2.0] - 2021-01-29

### Added
- Added `TaigaClient.upload_to_gcs` function that uploads a Taiga file to Google Cloud Storage

## [3.1.0] - 2021-01-28

### Changed
- When updating a dataset, local files to be uploaded are checked against the base dataset version. If they have the same content (based on SHA256 and MD5 hashes) and [type](./Definitions.md#DataFile_Type)/[format](./Definitions.md#DataFile_Formats) of a datafile in the base dataset version, the local file will not be uploaded, and instead a virtual file will be created based on the existing datafile.

### Fixed
- Removed checking for column types existing (endpoint now returns None)

## [3.0.5] - 2021-01-27

### Fixed
- Add warning when downloading a file whose underlying file is in a deprecated or deleted dataset

## [3.0.4] - 2021-01-26

### Fixed
- Use `low_memory_mode=False`

## [3.0.3] - 2021-01-26

### Fixed
- Lower version of pyarrow

## [3.0.2] - 2021-01-26

### Fixed
- Pin version of pyarrow

## [3.0.1] - 2021-01-21

### Fixed
- Fixed removing deleted datasets from the cache
- Fixed getting canonical Taiga IDs for short IDs referencing virtual datafiles

## [3.0.0] - 2021-01-21

### Added

- Ability to provide a map of Taiga IDs to Figshare download links to download files directly from Figshare.

### Changed

- `TaigaClient.download_to_cache` no longer supports a `format` parameter. The format of the file downloaded is now determined by the [datafile type](docs/Definitions.md#DataFile_Type) (CSV for "HDF5" and "Columnar" type, plaintext for "Raw" type).
- Creating and updating a dataset have new formats for the `upload_file` parameter. See [the API documentation](docs/TaigaClient%20API.md#taigapytaigaclientcreatedataset) for more details.
- Some no longer raise exceptions, and instead print an error message and return `None`. These include:
    - Attempting to fetch something from a deleted dataset
    - Attempting to fetch a Raw file
- Uploading files now occurs concurrently.

### Removed
- The `force` parameter is no longer available for various `TaigaClient` functions.
- UUID (ex: “dafc620dd7824a71a4b3ed42e4995d4e”) is no longer be a supported format for datasets, dataset versions, and datafiles (some may still work, but we won’t guarantee these anymore)
- `TaigaClient` no longer reads from raw CSVs in the Taiga cache

## [2.12.13] - 2020-05-13

### Fixed

- Actually fix deletion of corrupted virtual files from cache.
- Actually fix deletion of interrupted virtual file downloads from cache.

## [2.12.12] - 2020-04-29

### Fixed

- Fix deletion of corrupted virtual files from cache.
- Fix deletion of interrupted virtual file downloads from cache.

## [2.12.11] - 2020-02-13

### Added

- Add `TaigaClient.get_canonical_id` function, which gets the full (`dataset_permaname.dataset_version/datafile_name`) ID of the real (not virtual) datafile

## [2.12.10] - 2020-02-03

### Added
- Retrying `TaigaClient.get` when local HDF5 file is corrupted.
- Deleting file from cache if `TaigaClient.get` is interrupted by the user.

## [2.12.9] - 2020-01-24

### Fixed

- Set minimum version of progressbar

## [2.12.8] - 2019-11-06

### Fixed

- Fixed typo in updating a dataset with all files from previous version

## [2.12.7] - 2019-10-30

### Added

- New `TaigaClient.upload_dataset argument` `add_all_existing_files` which specifies whether to add existing files in the original dataset to the new dataset version as virtual files.
- New `TaigaClient.upload_dataset argument` `changes_description` which specifies the description of changes for the new dataset version.

## [2.12.6] - 2019-10-23

### Added

- `TaigaClient` keeps mapping of virtual datafiles to their underlying datafile, so `.get` and `.download_to_cache` will use the underlying file if it is in the cache, and no longer fetch the file Taiga.
- Matrix (HDF5) files are now downloaded as `.hdf5` for `TaigaClient.get` and taigaclient fetch, which avoids file conversions for unconverted files

## [2.12.5] - 2019-10-15

### Fixed

- Fix default for `TaigaClient.update_dataset` `upload_file_path_dict` argument

## [2.12.4] - 2019-10-15

### Fixed
- `TaigaClient.update_dataset` now allows updating a dataset with only virtual datafiles

## [2.12.3] - 2019-10-10

### Changed

- Use column type inference from pandas instead of Taiga

## [2.12.2] - 2019-10-03

### Added

- `taigaclient` dataset-meta command supports `--version-id`.

### Changed
- `taigaclient` handles and reports exceptions, rather than just printing them to console.
- Columnar datafile column type inference updated to use information from Taiga.


## [2.12.1] - 2019-10-03

### Changed
- `TaigaClient.get` supports datafile ID arguments in the form of `dataset_name.dataset_version` (previously raised exception)


## [2.12.0] - 2019-09-27

### Added

- `TaigaClient` offline mode, which uses files in the cache without validation when not connected to Taiga

## [2.11.0] - 2019-09-26

### Added

- `taigaclient` command line tool

### Changed

- `TaigaClient`'s cache uses feather format instead of pickling


[unreleased]: https://github.com/broadinstitute/taigapy/compare/3.3.1...HEAD
[3.3.1]: https://github.com/broadinstitute/taigapy/compare/3.3.0...3.3.1
[3.3.0]: https://github.com/broadinstitute/taigapy/compare/3.2.4...3.3.0
[3.2.4]: https://github.com/broadinstitute/taigapy/compare/3.2.3...3.2.4
[3.2.3]: https://github.com/broadinstitute/taigapy/compare/3.2.2...3.2.3
[3.2.2]: https://github.com/broadinstitute/taigapy/compare/3.2.1...3.2.2
[3.2.1]: https://github.com/broadinstitute/taigapy/compare/3.2.0...3.2.1
[3.2.0]: https://github.com/broadinstitute/taigapy/compare/3.1.0...3.2.0
[3.1.0]: https://github.com/broadinstitute/taigapy/compare/3.0.5...3.1.0
[3.0.5]: https://github.com/broadinstitute/taigapy/compare/3.0.4...3.0.5
[3.0.4]: https://github.com/broadinstitute/taigapy/compare/3.0.3...3.0.4
[3.0.3]: https://github.com/broadinstitute/taigapy/compare/3.0.2...3.0.3
[3.0.2]: https://github.com/broadinstitute/taigapy/compare/3.0.1...3.0.2
[3.0.1]: https://github.com/broadinstitute/taigapy/compare/3.0.0...3.0.1
[3.0.0]: https://github.com/broadinstitute/taigapy/compare/2.12.13...3.0.0
[2.12.13]: https://github.com/broadinstitute/taigapy/compare/2.12.12...2.12.13
[2.12.12]: https://github.com/broadinstitute/taigapy/compare/2.12.11...2.12.12
[2.12.11]: https://github.com/broadinstitute/taigapy/compare/2.12.10...2.12.11
[2.12.10]: https://github.com/broadinstitute/taigapy/compare/2.12.9...2.12.10
[2.12.9]: https://github.com/broadinstitute/taigapy/compare/2.12.8...2.12.9
[2.12.8]: https://github.com/broadinstitute/taigapy/compare/2.12.7...2.12.8
[2.12.7]: https://github.com/broadinstitute/taigapy/compare/2.12.6...2.12.7
[2.12.6]: https://github.com/broadinstitute/taigapy/compare/2.12.5...2.12.6
[2.12.5]: https://github.com/broadinstitute/taigapy/compare/2.12.4...2.12.5
[2.12.4]: https://github.com/broadinstitute/taigapy/compare/2.12.3...2.12.4
[2.12.3]: https://github.com/broadinstitute/taigapy/compare/2.12.2...2.12.3
[2.12.2]: https://github.com/broadinstitute/taigapy/compare/2.12.1...2.12.2
[2.12.1]: https://github.com/broadinstitute/taigapy/compare/2.12.0...2.12.1
[2.12.0]: https://github.com/broadinstitute/taigapy/compare/2.11.0...2.12.0
[2.11.0]: https://github.com/broadinstitute/taigapy/compare/ec0b5ee7ab302178dd6b23860abe7305aa447aa5...2.11.0
