# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed
- Pass through the "method" kwarg to DataFrameClient queries

### Removed

## [v5.2.0] - 2018-07-10
### Added
- Finally add a CHANGELOG.md to communicate breaking changes (#598)
- Test multiple versions of InfluxDB in travis
- Add SHARD DURATION parameter to retention policy create/alter
### Changed
- Update POST/GET requests to follow verb guidelines from InfluxDB documentation
- Update test suite to support InfluxDB v1.3.9, v1.4.2, and v1.5.4
- Fix performance degradation when removing NaN values via line protocol (#592)
### Removed
- Dropped support for Python3.4

## [v5.1.0] - 2018-06-26
### Added
- Connect to InfluxDB path running on server (#556 thx @gladhorn)
- Escape measurement names in DataFrameClient (#542 thx @tzonghao)
- Escape tags that end with a backslash (#537 thx @vaniakov)
- Add back mistakenly-dropped database parameter (#540)
- Add PyPI status to README.md
### Changed
- Fix bad session mount scheme (#571 thx @vaniakov)
- Fixed issue with DataFrameClient calling to_datetime function (#593 thx @dragoshenron)
- Escape columns in DataFrameClient for line protocol (#584 thx @dmuiruri)
- Convert DataFrameClient times from int to np.int64 (#495 thx patrickhoebeke)
- Updated pandas tutorial (#547 thx @techaddicted)
- Explicitly set numpy version for tox (#563)
### Removed
- Removed UDP precision restrictions on timestamp (#557 thx @mdhausman)

## [v5.0.0] - 2017-11-20
### Added
- Add pool size parameter to client constructor (#534 thx @vaniakov)
- Add ping method to client for checking connectivity (#409 thx @pmenglund)
- Add retry logic & exponential backoff when a connection fails (#508)
- Declare which setuptools version is required in PyPy env
- Functions for drop_measurement and get_list_measurements in InfluxDBClient (#402 thx @Vic020)
- Allow single string as data argument in write (#492 thx @baftek)
- Support chunked queries in DataFrameClient (#439 thx @gusutabopb)
- Add close method to InfluxDBClient (#465 thx @Linux-oiD)
- PEP257 linting & code compliance (#473)
### Changed
- Fix broken tags filtering on a ResultSet (#511)
- Improve retry codepath for connecting to InfluxDB (#536 thx @swails)
- Clean up imports using six instead of sys.version (#536 thx @swails)
- Replace references to dataframe.ix with dataframe.iloc (#528)
- Improve performance of tag processing when converting DataFrameClient to line protocol (#503 thx @tzonghao)
- Typo in Content-Type header (#513 thx @milancermak)
- Clean up README.md formatting
- Catch TypeError when casting to float to return False with objects (#475 thx @BenHewins)
- Improve efficiency of tag appending in DataFrameClient when converting to line protocol (#486 thx @maxdolle)
### Removed
- Drop requirement for all fields in SeriesHelper (#518 thx @spott)
- use_udp and udp_port are now private properties in InfluxDBClient

## [v4.1.1] - 2017-06-06
### Added
### Changed
### Removed

## [v4.1.0] - 2017-04-12
### Added
### Changed
### Removed

## [v4.0.0] - 2016-12-07
### Added
### Changed
### Removed

## [v3.0.0] - 2016-06-26
### Added
### Changed
### Removed

## [v2.12.0] - 2016-01-29
### Added
### Changed
### Removed

## [v2.11.0] - 2016-01-11
### Added
### Changed
### Removed

## [v2.10.0] - 2015-11-13
### Added
### Changed
### Removed

## [v2.9.3] - 2015-10-30
### Added
### Changed
### Removed

## [v2.9.2] - 2015-10-07
### Added
### Changed
### Removed

## [v2.9.1] - 2015-08-30
### Added
### Changed
### Removed

## [v2.9.0] - 2015-08-28
### Added
### Changed
### Removed

## [v2.8.0] - 2015-08-06
### Added
### Changed
### Removed

## [v2.7.3] - 2015-07-31
### Added
### Changed
### Removed

## [v2.7.2] - 2015-07-31
### Added
### Changed
### Removed

## [v2.7.1] - 2015-07-26
### Added
### Changed
### Removed

## [v2.7.0] - 2015-07-23
### Added
### Changed
### Removed

## [v2.6.0] - 2015-06-16
### Added
### Changed
### Removed

## [v2.5.1] - 2015-06-15
### Added
### Changed
### Removed

## [v2.5.0] - 2015-06-15
### Added
### Changed
### Removed

## [v2.4.0] - 2015-06-12
### Added
### Changed
### Removed

## [v2.3.0] - 2015-05-13
### Added
### Changed
### Removed

## [v2.2.0] - 2015-05-05
### Added
### Changed
### Removed

## [v2.1.0] - 2015-04-24
### Added
### Changed
### Removed

## [v2.0.2] - 2015-04-22
### Added
### Changed
### Removed

## [v2.0.1] - 2015-04-17
### Added
### Changed
### Removed

## [v2.0.0] - 2015-04-17
### Added
### Changed
### Removed

## [v1.0.1] - 2015-03-30
### Added
### Changed
### Removed

## [v1.0.0] - 2015-03-20
### Added
### Changed
### Removed

## [v0.4.1] - 2015-03-18
### Added
### Changed
### Removed

## [v0.4.0] - 2015-03-17
### Added
### Changed
### Removed

## [v0.3.1] - 2015-02-23
### Added
### Changed
### Removed

## [v0.3.0] - 2015-02-17
### Added
### Changed
### Removed

## [v0.2.0] - 2015-01-23
### Added
### Changed
### Removed

## [v0.1.13] - 2014-11-12
### Added
### Changed
### Removed

## [v0.1.12] - 2014-08-22
### Added
### Changed
### Removed

## [v0.1.11] - 2014-06-20
### Added
### Changed
### Removed

## [v0.1.10] - 2014-06-09
### Added
### Changed
### Removed

## [v0.1.9] - 2014-06-06
### Added
### Changed
### Removed

## [v0.1.8] - 2014-06-06
### Added
### Changed
### Removed

## [v0.1.7] - 2014-05-21
### Added
### Changed
### Removed

## [v0.1.6] - 2014-04-02
### Added
### Changed
### Removed

## [v0.1.5] - 2014-03-25
### Added
### Changed
### Removed

## [v0.1.4] - 2014-03-03
### Added
### Changed
### Removed

## [v0.1.3] - 2014-02-11
### Added
### Changed
### Removed

## [v0.1.2] - 2013-12-09
### Added
### Changed
### Removed

## [v0.1.1] - 2013-11-14
### Added
### Changed
### Removed
