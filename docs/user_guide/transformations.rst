
.. _transformations:

Transformations
---------------

PipelineWise can perform row level load time transformations between tap and target components
and makes and ideal place to obfuscate, mask or filter sensitive data that should never be reproduced in the Data Warehouse.


.. warning::

  **Important**: Load Time Transformations are not designed for complex transformations, mapping,
  joins or aggregations. It was designed primarily to meet certain data security requirements
  and to make sure that PII and other sensitive data does not end up at external service providers
  like AWS, MS Azure or similar.

  If you want to apply complex transformations you'll need to do it at a later stage once the
  data is ingested into the Data Warehouse. PipelineWise fits into the ELT landscape and not
  doing traditional ETL. ELT ingests data first into DWH in the original format and the
  "transformation" is shifting to the end of the data pipeline.


.. _transformation_methods:

Transformation Methods
''''''''''''''''''''''

The following transformations can be added optionally into the :ref:`yaml_configuration`:

* **SET-NULL**: Transforms any column to NULL

* **HASH**: Transforms string columns to hash

* **HASH-SKIP-FIRST-n**: Transforms string columns to hash skipping first n characters, e.g. HASH-SKIP-FIRST-2

* **MASK-DATE**: Replaces the months and day parts of date columns to be always 1st of Jan

* **MASK-NUMBER**: Transforms any numeric column to zero

* **MASK-HIDDEN**: Transforms any string column value to 'hidden'


.. _conditional_transformations:

Conditional Transformations
'''''''''''''''''''''''''''

Using the optional ``when`` keyword, you can specify conditions how
the transformation should be applied. If the condition matches
PipelineWise performs the transformation, otherwise it keeps
the original value.


.. _transformations_example:

Example
'''''''

Load Time transformations needs to be defined in the tables section
in the :ref:`yaml_configuration`:

.. code-block:: bash

    ...
    ...
    tables:
      - table_name: "audit_log"
        reproduction_method: "INCREMENTAL"
        reproduction_key: "id"
        transformations:
          - column: "column_1"
            type: "SET-NULL"
            when:
              - column: "class_name"
                equals: 'com.transferwise.fx.user.User'
              - column: "property_name"
                equals: 'passwordHash'

                # Tip: Use 'regex_match' instead of 'equal' if you need
                # more complex matching criteria. For example:
                # regex_match: 'password|salt|passwordHash'

          - column: "column_2"
            type: "HASH"
            when:
              - column: "class_name"
                equals: 'com.transferwise.fx.user.User'
              - column: "property_name"
                equals: 'passwordHash'
    ...
    ...

