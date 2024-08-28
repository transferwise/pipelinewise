8.2.1 (2023-11-28)
------------------

**Fixes**
- Close kafka consumer at the end of sync
- Commit offsets synchronously. 
- Bump `confluent-kafka[protobuf]` from `2.2.*` to `2.3.*`

8.2.0 (2023-11-17)
------------------
- Add more info logs
- Add new config [`debug_contexts`](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#debug-contexts) for enabling debugging

8.1.0 (2023-08-25)
------------------
- Bump `dpath` from `2.0.6` to `2.1.*`
- Bump `confluent-kafka[protobuf]` from `1.9.2` to `2.2.*`
- Bump `grpcio-tools` from `1.51.1` to `1.57.*`
- Bump test dependencies

8.0.0 (2022-12-08)
------------------
- Switch from `subscribe` to `assign` for better initial offset control
- Implement specifying partitions in configuration

7.1.1 (2022-10-18)
------------------
- Introducing the use of the `seek` method to reset the source partition offsets at the start of a run

7.1.0 (2022-07-14)
------------------
- Bump `pipelinewise-tap-kafka` from confluent-kafka from `1.8.2` to `1.9.0`.
- Remove CI checks for Python 3.6

7.0.0 (2022-03-29)
------------------
**BREAKING CHANGES**

Upgrading from 6.x to 7.x: For the current taps without any primary keys defined, set new configuration property `use_message_key` to `false`. Taps left with default settings and no custom primary keys specified will fail if kafka messages do not have keys and lead to unexpected behaviour on targets otherwise.

*Features*:
- Added support for the message keys to be used as primary key for the record. Using message key is now a default option where custom PKs are not defined.

6.0.0 (2022-03-17)
------------------

- Use unique proto class names per topic
- Raise exception on all brokers down
- Raise exception if custom PK not exists in the message
- Bump `dpath` from `2.0.5` to `2.0.6`
- Bump `grpcio-tools` from `1.43.0` to `1.44.0`
- Bump `pytest` from `6.2.5` to `7.0.1`

5.1.0 (2022-01-27)
------------------
*Features*:
  - Add protobuf support

*Fixes*:
  - Fixed an issue when log messages raised exceptions

*Requirement updates*:
  - Bump `dpath` from `2.0.1` to `2.0.5`
  - Bump `pytest-cov` from `2.10.1` to `3.0.0`
  - Bump `pylint` from `2.4.2` to `2.12.2`
  - Remove `filelock` requirement

5.0.1 (2022-01-26)
------------------

- Fixed an issue when `tap_kafka.serialization` module not included in the package

5.0.0 (2022-01-24)
------------------
**BREAKING CHANGES**

Upgrading from 4.x to 5.x: Please remove the existing `state.json` created by tap-kafka 4.x.
5.x will continue consuming messages from the last consumed offset but will generate `state.json` in a new format.

**CHANGELOG**
- Switching from `kafka-python` to `confluent-kafka-python`
- Using faster `orjson` provided by `pipelinewise-singer-python-2.x`
- Remove local store and bookmark consumed message in `STATE` messages
- Add `initial_start_time` optional parameter

4.0.1 (2021-08-13)
------------------
*Fixes*:
  * Fallback to default consumer timeout if `consumer_timeout_ms` is not provided in discovery mode.
  * Stop mis-handling exceptions during discovery.

4.0.0 (2020-08-27)
-------------------

- Improve the performance of persisting kafka messages if the local store cannot perform frequent file appends and causing high I/O issues
- Switching from `jsonpath-ng` to `dpath` python library to improve the performance of extracting primary keys
- Change the syntax of `primary_keys` from JSONPath to `/slashed/paths` ala XPath

3.1.0 (2020-04-20)
-------------------

- Add `max_poll_records` option

3.0.0 (2020-04-03)
-------------------

- Add local storage of consumed messages and instant commit kafka offsets
- Add more configurable options: `consumer_timeout_ms`, `session_timeout_ms`, `heartbeat_interval_ms`, `max_poll_interval_ms`
- Add two new fixed output columns: `MESSAGE_PARTITION` and `MESSAGE_OFFSET`

2.1.1 (2020-03-23)
-------------------

- Commit offset from state file and not from the consumed messages

2.1.0 (2020-02-18)
-------------------

- Make logging customisable

2.0.0 (2020-01-07)
-------------------

- Rewamp the output schema with no JSON flattening

1.0.2 (2019-11-25)
-------------------

- Add 'encoding' as a configurable parameter

1.0.1 (2019-08-16)
-------------------

- Add license classifier

1.0.0 (2019-06-03)
-------------------

- Initial release
