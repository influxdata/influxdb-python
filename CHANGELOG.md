# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [v5.3.2] - 2024-04-17

### Changed
- Correctly serialize nanosecond dataframe timestamps (#926)

## [v5.3.1] - 2022-11-14

### Added
- Add support for custom headers in the InfluxDBClient (#710 thx @nathanielatom)
- Add support for custom indexes for query in the DataFrameClient (#785)

### Changed
- Amend retry to avoid sleep after last retry before raising exception (#790 thx @krzysbaranski)
- Remove msgpack pinning for requirements (#818 thx @prometheanfire)
- Update support for HTTP headers in the InfluxDBClient (#851 thx @bednar)

### Removed

## [v5.3.0] - 2020-04-10

### Added
- Add mypy testing framework (#756)
- Add support for messagepack (#734 thx @lovasoa)
- Add support for 'show series' (#357 thx @gaker)
- Add support for custom request session in InfluxDBClient (#360 thx @dschien)
- Add support for handling np.nan and np.inf values in DataFrameClient (#436 thx @nmerket)
- Add support for optional `time_precision` in the SeriesHelper (#502 && #719 thx @appunni-dishq && @klDen)
- Add ability to specify retention policy in SeriesHelper (#723 thx @csanz91)
- Add gzip compression for post and response data (#732 thx @KEClaytor)
- Add support for chunked responses in ResultSet (#753 and #538 thx @hrbonz && @psy0rz)
- Add support for empty string fields (#766 thx @gregschrock)
- Add support for context managers to InfluxDBClient (#721 thx @JustusAdam)

### Changed
- Clean up stale CI config (#755)
- Add legacy client test (#752 & #318 thx @oldmantaiter & @sebito91)
- Update make_lines section in line_protocol.py to split out core function (#375 thx @aisbaa)
- Fix nanosecond time resolution for points (#407 thx @AndreCAndersen && @clslgrnc)
- Fix import of distutils.spawn (#805 thx @Hawk777)
- Update repr of float values including properly handling of boolean (#488 thx @ghost)
- Update DataFrameClient to fix faulty empty tags (#770 thx @michelfripiat)
- Update DataFrameClient to properly return `dropna` values (#778 thx @jgspiro)
- Update DataFrameClient to test for pd.DataTimeIndex before blind conversion (#623 thx @testforvin)
- Update client to type-set UDP port to int (#651 thx @yifeikong)
- Update batched writing support for all iterables (#746 thx @JayH5)
- Update SeriesHelper to enable class instantiation when not initialized (#772 thx @ocworld)
- Update UDP test case to add proper timestamp to datapoints (#808 thx @shantanoo-desai)

### Removed

## [v5.2.3] - 2019-08-19

### Added
- Add consistency param to InfluxDBClient.write_points (#643 thx @RonRothman)
- Add UDP example (#648 thx @shantanoo-desai)
- Add consistency paramter to `write_points` (#664 tx @RonRothman)
- The query() function now accepts a bind_params argument for parameter binding (#678 thx @clslgrnc)
- Add `get_list_continuous_queries`, `drop_continuous_query`, and `create_continuous_query` management methods for
  continuous queries (#681 thx @lukaszdudek-silvair && @smolse)
- Mutual TLS authentication (#702 thx @LloydW93)

### Changed
- Update test suite to add support for Python 3.7 and InfluxDB v1.6.4 and 1.7.4 (#692 thx @clslgrnc)
- Update supported versions of influxdb + python (#693 thx @clslgrnc)
- Fix for the line protocol issue with leading comma (#694 thx @d3banjan)
- Update classifiers tuple to list in setup.py (#697 thx @Hanaasagi)
- Update documentation for empty `delete_series` confusion (#699 thx @xginn8)
- Fix newline character issue in tag value (#716 thx @syhan)
- Update tests/tutorials_pandas.py to reference `line` protocol, bug in `json` (#737 thx @Aeium)

### Removed

## [v5.2.2] - 2019-03-14
### Added

### Changed
- Fix 'TypeError: Already tz-aware' introduced with recent versions of Panda (#671, #676, thx @f4bsch @clslgrnc)

## [v5.2.1] - 2018-12-07
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
