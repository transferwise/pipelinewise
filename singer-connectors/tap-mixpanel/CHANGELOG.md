1.7.1 (2026-02-09)
-------------------

- Rebased `tap-mixpanel` implementation to upstream `1.7.1` (released 2025-02-20)
- Added support for Mixpanel EU domain selection via `eu_residency` and `eu_residency_server`
- Added support for `request_timeout` and optional `end_date` config params from upstream
- Improved HTTP error handling and retryability for 429 rate-limit responses
- Preserved compatibility for `denest_properties` and list/string `export_events` config values

1.2.0 (2020-10-29)
-------------------

- Add optional `denest_properties` optional param
- Fix key properties in the export stream

1.1.0 (2020-10-29)
-------------------

- Add key properties to export stream
- Add `mp_reserved_insert_id` to export stream

1.0.1 (2020-10-27)
-------------------

- Republish to PyPI to have long description

1.0.0 (2020-10-27)
-------------------

- Add `export_events` optional param
- Remove `page_size` param when checking access
- Few more info log
- Initial fork of https://github.com/singer-io/tap-mixpanel 1.1.0
