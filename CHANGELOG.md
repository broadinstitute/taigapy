# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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


[unreleased]: https://github.com/broadinstitute/taigapy/compare/2.12.13...HEAD
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