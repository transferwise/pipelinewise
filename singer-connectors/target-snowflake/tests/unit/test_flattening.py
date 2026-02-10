import unittest

import target_snowflake.flattening as flattening


class TestFlattening(unittest.TestCase):

    def setUp(self):
        self.config = {}

    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = flattening.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        self.assertEqual(flatten_schema(schema_with_no_properties), {})

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}

        # NO FLATTENING - Schema with simple properties should be a plain dictionary
        self.assertEqual(flatten_schema(not_nested_schema), not_nested_schema['properties'])

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        self.assertEqual(flatten_schema(nested_schema_with_no_properties),
                          nested_schema_with_no_properties['properties'])

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties), nested_schema_with_properties['properties'])

        # NO FLATTENING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=0),
                          nested_schema_with_properties['properties'])

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=1),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3': {
                                  'type': ['null', 'object'],
                                  "properties": {
                                      "multi_nested_prop1": {"type": ["null", "string"]},
                                      "multi_nested_prop2": {"type": ["null", "string"]}
                                  }
                              }
                          })

        # FLATTENING - Schema with object type property but without further properties should be a dict with
        # flattened properties
        self.assertEqual(flatten_schema(nested_schema_with_properties, max_level=10),
                          {
                              'c_pk': {'type': ['null', 'integer']},
                              'c_varchar': {'type': ['null', 'string']},
                              'c_int': {'type': ['null', 'integer']},
                              'c_obj__nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop2': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                              'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
                          })

        salesforce_history_schema = {
            "type": "object",
            "properties": {
                "Id": {"type": ["null", "string"]},
                "Field": {"type": ["null", "string"]},
                "OldValue": {},
                "NewValue": {}
            }
        }

        # Salesforce history fields OldValue/NewValue are anyType (no explicit type in schema).
        # We must keep them so they are created and loaded in Snowflake.
        self.assertEqual(
            flatten_schema(salesforce_history_schema),
            {
                "Id": {"type": ["null", "string"]},
                "Field": {"type": ["null", "string"]},
                "OldValue": {"type": ["null", "string"]},
                "NewValue": {"type": ["null", "string"]}
            }
        )

    def test_anyof_object_schema_uses_same_flattening_as_object_type_schema(self):
        """anyOf object schemas should recurse like direct object type schemas."""
        flatten_schema = flattening.flatten_schema

        object_type_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop": {"type": ["null", "string"]}
                    }
                }
            }
        }

        anyof_object_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_obj": {
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "nested_prop": {"type": ["null", "string"]}
                            }
                        },
                        {"type": ["null", "string"]}
                    ]
                }
            }
        }

        self.assertEqual(
            flatten_schema(anyof_object_schema, max_level=1),
            flatten_schema(object_type_schema, max_level=1)
        )

    def test_salesforce_history_populated_old_and_new_values_are_preserved(self):
        """Salesforce history rows with populated OldValue/NewValue should be retained."""
        flatten_schema = flattening.flatten_schema
        flatten_record = flattening.flatten_record

        salesforce_history_schema = {
            "type": "object",
            "properties": {
                "Id": {"type": ["null", "string"]},
                "Field": {"type": ["null", "string"]},
                "OldValue": {},
                "NewValue": {}
            }
        }

        # Example values observed in Salesforce history discussions:
        # Account lookup changes can appear either as IDs or labels.
        id_variant = {
            "Id": "00kxx0000001234AAA",
            "Field": "Account",
            "OldValue": "0016300000fDQRdAAO",
            "NewValue": "0016300000fDQReAAO"
        }
        label_variant = {
            "Id": "00kxx0000001235AAA",
            "Field": "Account",
            "OldValue": "Fred Fubar",
            "NewValue": "Francis Fubar"
        }

        flattened_schema = flatten_schema(salesforce_history_schema)
        self.assertEqual(flattened_schema["OldValue"], {"type": ["null", "string"]})
        self.assertEqual(flattened_schema["NewValue"], {"type": ["null", "string"]})

        flat_id_variant = flatten_record(id_variant, flattened_schema)
        flat_label_variant = flatten_record(label_variant, flattened_schema)

        self.assertEqual(flat_id_variant["OldValue"], "0016300000fDQRdAAO")
        self.assertEqual(flat_id_variant["NewValue"], "0016300000fDQReAAO")
        self.assertEqual(flat_label_variant["OldValue"], "Fred Fubar")
        self.assertEqual(flat_label_variant["NewValue"], "Francis Fubar")

    def test_legacy_flatten_schema_dropped_salesforce_anytype_fields(self):
        """
        Replicates the previous flatten_schema logic to show why OldValue/NewValue were not exported:
        when a property was `{}` (no type/no values), it wasn't emitted into the flattened schema.
        """
        flatten_schema = flattening.flatten_schema

        salesforce_history_schema = {
            "type": "object",
            "properties": {
                "Id": {"type": ["null", "string"]},
                "Field": {"type": ["null", "string"]},
                "OldValue": {},
                "NewValue": {}
            }
        }

        def legacy_flatten_schema_behavior(schema):
            items = {}
            for key, value in schema["properties"].items():
                if "type" in value:
                    items[key] = value
                elif len(value.values()) > 0:
                    first_value = list(value.values())[0]
                    if isinstance(first_value, list) and first_value and isinstance(first_value[0], dict):
                        value_type = first_value[0].get("type")
                        if value_type in ["string", "array", "object"]:
                            promoted = dict(first_value[0])
                            promoted["type"] = ["null", value_type]
                            items[key] = promoted
            return items

        legacy_schema = legacy_flatten_schema_behavior(salesforce_history_schema)
        current_schema = flatten_schema(salesforce_history_schema)

        # Legacy behavior dropped these fields entirely, which means downstream loaders never create/load them.
        self.assertNotIn("OldValue", legacy_schema)
        self.assertNotIn("NewValue", legacy_schema)

        # Current behavior keeps the fields so they can be materialized into Snowflake columns.
        self.assertIn("OldValue", current_schema)
        self.assertIn("NewValue", current_schema)
        self.assertEqual(current_schema["OldValue"], {"type": ["null", "string"]})
        self.assertEqual(current_schema["NewValue"], {"type": ["null", "string"]})

    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = flattening.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        self.assertEqual(flatten_record(empty_record), {})

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENING - Record with simple properties should be a plain dictionary
        self.assertEqual(flatten_record(not_nested_record), not_nested_record)

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENING - No flattening (default)
        self.assertEqual(flatten_record(nested_record),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
                                       '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
                          })

        # NO FLATTENING
        #   max_level: 0 : No flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=0),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {'
                                       '"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
                          })

        # SEMI FLATTENING
        #   max_level: 1 : Semi-flattening (default)
        self.assertEqual(flatten_record(nested_record, max_level=1),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": '
                                                     '"multi_value_2"}'
                          })

        # FLATTENING
        self.assertEqual(flatten_record(nested_record, max_level=10),
                          {
                              "c_pk": 1,
                              "c_varchar": "1",
                              "c_int": 1,
                              "c_obj__nested_prop1": "value_1",
                              "c_obj__nested_prop2": "value_2",
                              "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                              "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
                          })

    def test_flatten_record_with_flatten_schema(self):
        flatten_record = flattening.flatten_record

        flatten_schema = {
            "id": {
                "type": [
                    "object",
                    "array",
                    "null"
                ]
            }
        }

        test_cases = [
            (
                True,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": "1",
                    "data": "xyz"
                }
            ),
            (
                False,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": 1,
                    "data": "xyz"
                }
            )
        ]

        for idx, (should_use_flatten_schema, record, expected_output) in enumerate(test_cases):
            output = flatten_record(record, flatten_schema if should_use_flatten_schema else None)
            self.assertEqual(output, expected_output, f"Test {idx} failed. Testcase: {test_cases[idx]}")
