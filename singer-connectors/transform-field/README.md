# transform-field

Transformation component between [Singer](https://www.singer.io/) taps and targets.

### To run

Put it between a tap and a target with simple unix pipes:

`some-singer-tap | transform-field --transformations [transformations.json] | some-singer-target`

### Configuration

You need to defines which columns have to be transformed by which method and in which condition the transformation has to be applied.

**Configuring directly from JSON**:

(Tip: PipelineWise generating this for you from a more readable YAML format)

```
  {
    "transformations": [
        {
            "field_id": "password_hash",
            "tap_stream_name": "stream-id-sent-by-the-tap",
            "type": "SET-NULL"
        },
        {
            "field_id": "salt",
            "tap_stream_name": "stream-id-sent-by-the-tap",
            "type": "SET-NULL"
        },
        {
            "field_id": "value",
            "tap_stream_name": "stream-id-sent-by-the-tap",
            "type": "SET-NULL"
            'when': [
                {'column': 'string_column_1', 'equals': "Property" },
                {'column': 'numeric_column', 'equals': 200 },
                {'column': 'string_column_2', 'regex_match': 'sensitive.*PII' },
              ]
        }

    ]
}
```

**Configuring from PipelineWise tap YAML**:
```
      tables:
      - table_name: "audit_log"
        replication_method: "INCREMENTAL"
        replication_key: "id"
        transformations:
          - column: "old_value"
            type: "SET-NULL"
            when:
              - column: "class_name"
                equals: 'com.transferwise.fx.user.User'
              - column: "property_name"
                equals: 'passwordHash'

                # Tip: Use 'regex_match' instead of 'equal' if you need
                # more complex matching criterias. For example:
                # regex_match: 'password|salt|passwordHash'

          - column: "new_value"
            type: "SET-NULL"
            when:
              - column: "class_name"
                equals: 'com.transferwise.fx.user.User'
              - column: "property_name"
                equals: 'passwordHash'
```

### Transformation types

* **SET-NULL**: Transforms any input to NULL
* **HASH**: Transfroms string input to hash
* **HASH-SKIP-FIRST-n**: Transforms string input to hash skipping first n characters, e.g. HASH-SKIP-FIRST-2
* **MASK-DATA**: Transforms any date to stg
* **MASK-NUMBER**: Transforms any number to zero

### To run tests:

1. Install python dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
  pip install nose
```

1. To run tests:
```
  nosetests --where=tests
```

