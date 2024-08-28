import os
import singer
import jsonschema

class TestSchemas:
    """ Test schemas """
    schemas_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tap_slack', 'schemas'))

    def get_schemas(self):
        """Function to get all json schema paths"""
        schemas = []
        for f in os.listdir(self.schemas_dir):
            if os.path.isfile(os.path.join(self.schemas_dir, f)):
                if '.json' in f:
                    schemas.append(os.path.join(self.schemas_dir, f))

        return schemas

    def test_schemas(self):
        """Check if every schema is a valid JSON Schema"""
        for schema in self.get_schemas():
            s = singer.utils.load_json(schema)
            assert jsonschema.Draft7Validator(s).check_schema(s) is None
