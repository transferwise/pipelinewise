3.0.1 (2023-07-03)
------------------

**Changes**
   - Bump `boto3` from `1.26.21` to `1.26.165`

**Fix**
   - Filter out none standard storage class objects


3.0.0 (2022-12-02)
------------------

**Changes**
  - Dropping csv type columns' guessing logic using `messytables` library in favor of interpreting all columns as string to avoid type mismatch issues that break the tap/target.

2.0.0 (2022-02-10)
------------------

**Changes**
  - Dropped support for python 3.6
  - Bump ujson from 4.3.0 to 5.1.0

1.2.3 (2022-01-14)
------------------
**Fix**
  - Set `time_extracted` when creating singer records.

**Changes**
  - Migrate CI to github actions
  - bump dependencies

1.2.2 (2021-07-19)
------------------
**Fix**
  - Make use of `start_date` when doing discovery
  - Discovery to run on more recent files to be able to detect new columns.

1.2.1 (2021-04-22)
------------------
- Bumping dependencies

1.2.0 (2020-08-04)
------------------
- Add `aws_profile` option to support Profile based authentication to S3
- Add option to authenticate to S3 using `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables

1.1.0 (2020-02-20)
------------------
- Make logging configurable

1.0.7 (2020-01-07)
------------------
- Updated generated json schema to be more in sync with fast sync in PipelineWise

1.0.6 (2019-12-04)
------------------
- New data type guesser by `messytables`

1.0.5 (2019-09-10)
------------------
- Add `aws_endpoint_url` to support non-aws S3 account

1.0.4 (2019-08-16)
------------------
- License classifier and project description update

1.0.3 (2019-05-13)
------------------
- Raise exception when file(s) cannot sample

1.0.2 (2019-05-09)
------------------
- Better error messages when no files found

1.0.0 (2019-05-08)
------------------
- Initial release
