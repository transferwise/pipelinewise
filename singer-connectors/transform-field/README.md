# pipelinewise-transform-field

[![PyPI version](https://badge.fury.io/py/pipelinewise-transform-field.svg)](https://badge.fury.io/py/pipelinewise-transform-field)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-transform-field.svg)](https://pypi.org/project/pipelinewise-transform-field/)
[![License: Apache2](https://img.shields.io/badge/License-Apache2-yellow.svg)](https://opensource.org/licenses/Apache-2.0)

Transformation component between [Singer](https://www.singer.io/) taps and targets.

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible component.

## How to use it

The recommended method of running this component is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files, and most of things are automated. 
Please check the related documentation at [Transformations](https://transferwise.github.io/pipelinewise/user_guide/transformations.html)

If you want to run this [Singer](https://singer.io) compatible component independently please read further.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-transform-field
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip setuptools
  pip install .
```

### To validate transformations

`transform-field --validate --config [config.json] --catalog [catalog.json]`

### To run

Put it between a tap and a target with simple unix pipes:

`some-singer-tap | transform-field --config [config.json] | some-singer-target`

It's reading incoming messages from STDIN and using `config.json` to transform incoming RECORD messages.

**Note**: To avoid version conflicts run `tap`, `transform` and `targets` in separate virtual environments.

### Transformation types

The following are the transformation types supported by _pipelinewise-transform-field_:

* **SET-NULL**: Transforms any input to NULL
* **HASH**: Transforms string input to hash
* **HASH-SKIP-FIRST-n**: Transforms string input to hash skipping first n characters, e.g. HASH-SKIP-FIRST-2
* **MASK-DATE**: Replaces the months and day parts of date columns to be always 1st of Jan
* **MASK-NUMBER**: Transforms any numeric value to zero
* **MASK-HIDDEN**: Transforms any string to 'hidden'
* **MASK-STRING-SKIP-ENDS-n**: Transforms string input to masked version skipping first and last n characters, e.g. MASK-STRING-SKIP-ENDS-3

_PS_: 1 =< n =< 9

### Conditional transformations

It is possible to transform a record's property based on some given condition(s), the transformation will only take place when all conditions are met.

A condition is a combination of: 
* column [required]: the field to look up to
* operation [required]: the comparison type to use, the supported ones are `equals` and `regex_match`.
* value [required]: the column value to look for in records.

**An equality condition on a column**
```json
{
  "column": "<some column name>",
  "equals": <some important value>
}
```

**A regex condition on a column**
```json
{
  "column": "<some column name>",
  "regex_match": "<some regex pattern>"
}
```

**A condition on a property within a JSON-type column**
```json
{
  "column": "<some column name>",
  "field_path": "<xpath to property within 'column' object>",
  "equals": <some important value>
}
```

### Configuration

You need to define which columns have to be transformed by which method and in which condition the transformation needs to be applied.

#### Basic transformation
A basic transformation is where a field in all a stream records will be transformed can be achieved with:
```json
{
  "tap_stream_name": "<stream ID>",
  "field_id": "<Name of the field to transform in the record>",
  "type": "<Transformation type>"
}
```

#### Transformation within JSON

In order to transform property(ies) within a JSON type field, you can make use of `field_paths` property:

```json
{
  "tap_stream_name": "<stream ID>",
  "field_id": "<Name of the field to transform in the record>",
  "field_paths": ["xpath to property 1", "xpath to property 2"],
  "type": "<Transformation type>"
}
```

#### Conditional Transformation

To apply transformation conditionally, you can make use of the property `when` which can have one or many conditions:

```json
{
  "tap_stream_name": "<stream ID>",
  "field_id": "<Name of the field to transform in the record>",
  "type": "<Transformation type>",
  "when": [
    {"column": "string_col_1", "equals": "some value"},
    {"column": "string_col_2", "regex_match": ".*PII.*"},
    {"column": "numeric_col_1", "equals": 33},
    {"column": "json_column", "field_path": "metadata/comment", "regex_match": "sensitive"}
  ]
}
```

**Sample config** 
[config.json](./sample_config.json)

(Tip: PipelineWise generating this for you from a more readable YAML format)


### To check code style:

1. Install python dependencies in a virtual env
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip setuptools
  pip install .[test]
```

2. Run pylint
```shell
pylint transform_field
```

### To run tests:

1. Install python dependencies in a virtual env and run unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip setuptools
  pip install .[test]
```

2. Run tests:

* Unit tests
```
  pytest -v tests/unit
```

* Integration tests
```
  pytest -v tests/integration
```

* All tests
```
  pytest -v tests
```



## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.

