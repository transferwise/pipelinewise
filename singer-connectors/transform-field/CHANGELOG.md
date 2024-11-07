# CHANGELOG

## 2.3.0 (2021-12-16)
### Added
- Transformation of specific fields in object/array type properties in `RECORD` by using XPath syntax.
- Conditions on specific fields in object/array type properties in `RECORD`.

## 2.2.0 (2021-09-17)
### Added
- New transformation MASK-STRING-SKIP-ENDS-n. The transformation masks the string except start and end n-characters.

## 2.1.0 (2021-03-11)
### Addedd
- `--validate` flag to do one-off validatation of the transformation config using a given catalog file.

### Changed
- Validation of the transformation during runtime whenever a new `SCHEMA` type message has been received.


## 2.0.0 (2020-03-17)

### Changed
- Stop trimming transformed values
