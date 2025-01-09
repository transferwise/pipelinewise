2.0.0 (2022-06-13)
-------------------
*Breaking changes*

- Drop support for Python 3.6

*Fixes*

- FileNotFoundError when sync runs for more than a day 
- State nullified after every record 

1.5.0 (2021-08-13)
-------------------
- Add support for optional `aws_endpoint_url` to the configuration
- Bumping dependencies

1.4.0 (2020-09-09)
-------------------

- Add `temp_dir` option to write temporary files to custom location

1.3.0 (2020-07-28)
-------------------

- Add `aws_session_token` and `aws_profile` options for Profile based authentication
- Add `naming_convention` option with `{date}`, `{stream}` and `{timestamp}` tokens to create dynamic file names on S3

1.2.1 (2020-06-17)
-------------------

- Switch jsonschema to use Draft7Validator

1.2.0 (2020-02-18)
-------------------

- Support custom logging configuration by setting `LOGGING_CONF_FILE` env variable to the absolute path of a .conf file

1.1.0 (2020-01-28)
-------------------

- Add gzip file support
- Add support for KMS encryption

1.0.1 (2019-08-16)
-------------------

- Add license details

1.0.0 (2019-06-03)
-------------------

- Initial commit
