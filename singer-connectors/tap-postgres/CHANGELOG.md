2.1.0 (2023-03-30)
-------------------
**Changes**
- INCREMENTAL: An optional config `limit` to be appended to incremental queries to limit their runtime.  

**Fixes**
- INCREMENTAL: `ORDER BY` added back to query in case replication key value is None. 

2.0.0 (2022-11-02)
-------------------
**Changes**
- LOG_BASED: Use wal2json format-version v2 to read WAL, require wal2json >= 2.3 to be installed on pg server.
- Bump `psycopg2-binary` from `2.9.4` to `2.9.5`

1.8.4 (2022-09-08)
-------------------
**Changes**
- INCREMENTAL: Use sub-query to trick PostreSQL into more efficient use of index.

1.8.3 (2022-01-18)
-------------------
**Fixes**
- INCREMENTAL: generate valid SQL to extract data from tables where no `replication_key_value` in the state file.

1.8.2 (2022-01-14)
-------------------
**Fixes**
- LOG_BASED: catch exceptions and emit current state upon exiting
- LOG_BASED: `max_run_seconds` only working when there are wal messages.
- LOG_BASED: Prevent high CPU utilization while waiting for wal messages.

**Changes**
- local database container for dev and test purposes.
- bump `psycopg2-binary` from `2.8.6` to `2.9.3`


1.8.1 (2021-09-23)
-------------------
**Fixes**
- LOG_BASED: Handle dates with year > 9999.
- INCREMENTAL & FULL_TABLE: Avoid processing timestamps arrays as timestamp

1.8.0 (2021-06-23)
-------------------

- Add discovering of partitioned tables.

1.7.1 (2020-10-13)
-------------------

- Parse data from json(b) when converting a row to a record message in log based replication method.

1.7.0 (2020-09-21)
-------------------

- Enable SSL mode if `ssl` option is 'true' in config

1.6.4 (2020-09-21)
-------------------

- Ensure generated Postgres replication slot name is valid
- Handle timestamps out of range

1.6.3 (2020-07-10)
-------------------

Fix data loss issue when running `LOG_BASED` due to the tap not sending new SCHEMA singer messages when source tables change structure, mainly new/renamed columns, which causes the target to not be up to date with the stream structure.
The tap now:
* Runs discovery for selected stream at the beginning of sync to send up to date SCHEMA singer messages
* When new columns are detected in WAL payloads, then run discovery for the stream and send new SCHEMA message.

1.6.2 (2020-05-18)
-------------------

- Fixed issue when JSON type not converted to dictionary

1.6.1 (2020-05-07)
-------------------

- Fixed an issue when existing replication slot not found

1.6.0 (2020-04-20)
-------------------

- Enable `LOG_BASED` to replicate multiple databases by multiple taps
- Fix extracting data from tables with space in the name

1.5.3 (2020-03-31)
-------------------

- Minor loggin change

1.5.2 (2020-03-10)
-------------------

- Backward compatibility fix for PG version > 10

1.5.1 (2020-03-10)
-------------------

- Support session `wal_sender_timeout` setting

1.4.1 (2020-02-27)
-------------------

- Remove unused timestamps in logical replication

1.4.0 (2020-02-19)
-------------------

- Make logging customizable

1.3.1 (2020-01-08)
-------------------

- Support for jsonb column type

1.3.0 (2019-11-15)
-------------------

- Add `break_at_end_lsn` setting
- Add `max_run_seconds` setting
- Only send feedback when `lsn_comitted` has increased
- Remove some incompatible singer tap-postgres code

1.2.1 (2020-11-11)
-------------------

- Minor error handling optimization

1.2.0 (2020-11-04)
-------------------

- Bump psycopg2 to `2.8.4`
- Rely on psycopg2 to send keep-alive status updates to source

1.1.9 (2020-10-18)
-------------------

- Fix error handling condition if state file cannot be opened to read latest commit position

1.1.8 (2020-10-14)
-------------------

- Remove database_name from stream-id
- Remove LOG_BASED stream bookmarks from state if it has been de-selected in catalogue
- Convert values in time with timezone columns to UTC

1.1.7 (2020-10-04)
-------------------

- Bug fixes and stability improvements

1.1.6 (2020-09-20)
-------------------

- Remove include_schemas_in_destination_stream_name and always add schema name to stream
- Fix regression bug
- Only request selected tables from wal2json

1.1.5 (2020-09-09)
-------------------

- Untracked dev changes

1.1.2 (2020-08-12)
-------------------

- Untracked dev changes

1.1.1 (2020-07-25)
-------------------

- Bump psycopg2 to `2.8.3`
- Also very fast, but due to the PostgreSQL session memory limit, will fail for very large transactions

1.1.0 (2020-07-24)
-------------------

- Untracked dev changes

1.0.9 (2020-07-17)
-------------------

- Untracked dev changes

1.0.8 (2020-07-11)
-------------------

- Keep-alive feedback message sent to the source server is now only sent every 5 seconds

1.0.7 (2020-07-01)
-------------------

- Lsn position is no longer flushed at the end of the run, but only at the start of the next run

1.0.6 (2020-07-01)
-------------------

- Only search for DB specific slot
- Improve version detection and control
- Clarify Update Bookmark Period

1.0.5 (2020-06-27)
-------------------

- Lower default `poll_total_seconds`
- When no data is received, poll every 5 seconds for 15 seconds total
- Do not flush lsn at the end of a run, but at the start of the next run

1.0.4 (2020-06-25)
-------------------

- Some postgres instances do not have hstore available. do not make that assumption
- hstore rec must exist && must have an installed version to use hstore
- Send all stream schemas before syncing for binlog
- dsn string was causing encoding issues for a client with backslashes in their password
- Change bookmark property from primary key to replication key
- Only fetch the end_lsn if a log based stream is in play.  aurora, for instances, does not support lsns
- Do NOT consume message if its lsn > the global end_lsn for that tap run
- Don't break logical replication just because we haven't seen a message in 10 seconds

1.0.3 (2020-05-28)
-------------------

- Initial release with updated `README.md`








