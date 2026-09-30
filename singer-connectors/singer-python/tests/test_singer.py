import singer
import orjson
import unittest
import dateutil


class TestSinger(unittest.TestCase):
    def test_parse_message_record_good(self):
        message = singer.parse_message(
            '{"type": "RECORD", "record": {"name": "foo"}, "stream": "users"}')
        self.assertEqual(
            message,
            singer.RecordMessage(record={'name': 'foo'}, stream='users'))

    def test_parse_message_record_with_version_good(self):
        message = singer.parse_message(
            '{"type": "RECORD", "record": {"name": "foo"}, "stream": "users", "version": 2}')
        self.assertEqual(
            message,
            singer.RecordMessage(record={'name': 'foo'}, stream='users', version=2))

    def test_parse_message_record_naive_extraction_time(self):
        with self.assertRaisesRegex(ValueError, 'must be either None or an aware datetime'):
            singer.parse_message(
                '{"type": "RECORD", "record": {"name": "foo"}, "stream": "users", "version": 2'
            ', "time_extracted": "1970-01-02T00:00:00"}')

    def test_parse_message_record_aware_extraction_time(self):
        message = singer.parse_message(
            '{"type": "RECORD", "record": {"name": "foo"}, "stream": "users", "version": 2'
            ', "time_extracted": "1970-01-02T00:00:00.000Z"}')
        expected = singer.RecordMessage(
            record={'name': 'foo'},
            stream='users',
            version=2,
            time_extracted=dateutil.parser.parse('1970-01-02T00:00:00.000Z'))
        print(message)
        print(expected)
        self.assertEqual(message, expected)

    def test_extraction_time_strftime(self):
        """ Test that we're not corrupting timestamps with cross platform parsing. (Test case for OSX, specifically) """
        message = singer.RecordMessage(
            record={'name': 'foo'},
            stream='users',
            version=2,
            time_extracted=dateutil.parser.parse('1970-01-02T00:00:00.000Z'))
        expected = '1970-01-02T00:00:00.000000Z'
        self.assertEqual(message.asdict()['time_extracted'], expected)

    def test_parse_message_record_missing_record(self):
        with self.assertRaises(Exception):
            singer.parse_message('{"type": "RECORD", "stream": "users"}')

    def test_parse_message_record_missing_stream(self):
        with self.assertRaises(Exception):
            singer.parse_message(
                '{"type": "RECORD", "record": {"name": "foo"}}')

    def test_parse_message_schema_good(self):
        message = singer.parse_message('{"type": "SCHEMA", "stream": "users"'
            ', "schema": {"type": "object", "properties": {"name": {"type": "string"}}}'
            ', "key_properties": ["name"]}')
        self.assertEqual(
            message,
            singer.SchemaMessage(
                stream='users',
                key_properties=['name'],
                schema={'type': 'object',
                        'properties': {
                            'name': {'type': 'string'}}}))

    def test_parse_message_schema_missing_stream(self):
        with self.assertRaises(Exception):
            singer.parse_message('{"type": "SCHEMA"'
            ', "schema": {"type": "object", "properties": {"name": {"type": "string"}}}'
            ', "key_properties": ["name"]}')

    def test_parse_message_schema_missing_schema(self):
        with self.assertRaises(Exception):
            singer.parse_message(
                '{"type": "SCHEMA", "stream": "users"'
            ', "key_properties": ["name"]}')

    def test_parse_message_schema_missing_key_properties(self):
        with self.assertRaises(Exception):
            singer.parse_message('{"type": "SCHEMA", "stream": "users"'
            ', "schema": {"type": "object", "properties": {"name": {"type": "string"}}}}')

    def test_parse_message_state_good(self):
        message = singer.parse_message(
            '{"type": "STATE", "value": {"seq": 1}}')
        self.assertEqual(message, singer.StateMessage(value={'seq': 1}))

    def test_parse_message_state_missing_value(self):
        with self.assertRaises(Exception):
            singer.parse_message('{"type": "STATE"}')

    def test_parse_message_batch_good(self):
        message = singer.parse_message(
            '{"type": "BATCH", "stream": "users", "filepath": "/tmp/users0001.jsonl", "format": "jsonl"'
            ', "time_extracted": "1970-01-02T00:00:00.000Z"}')
        self.assertEqual(
            message,
            singer.BatchMessage(stream='users', filepath='/tmp/users0001.jsonl',
                                time_extracted=dateutil.parser.parse('1970-01-02T00:00:00.000Z'))
        )

    def test_parse_message_batch_missing_value(self):
        with self.assertRaises(Exception):
            singer.parse_message('{"type": "BATCH"}')

    def test_round_trip(self):
        record_message = singer.RecordMessage(
            record={'name': 'foo'},
            stream='users')

        schema_message = singer.SchemaMessage(
            stream='users',
            key_properties=['name'],
            schema={'type': 'object',
                    'properties': {
                        'name': {'type': 'string'}}})

        state_message = singer.StateMessage(value={'seq': 1})

        self.assertEqual(record_message,
                         singer.parse_message(singer.format_message(record_message)))
        self.assertEqual(schema_message,
                         singer.parse_message(singer.format_message(schema_message)))
        self.assertEqual(state_message,
                         singer.parse_message(singer.format_message(state_message)))

    ## These three tests just confirm that writing doesn't throw
    def test_write_record(self):
        singer.write_record('users', {'name': 'mike'})

    def test_write_schema(self):
        schema={'type': 'object',
                'properties': {
                    'name': {'type': 'string'}}}
        singer.write_schema('users', schema, ['name'])

    def test_write_state(self):
        singer.write_state({'foo': 1})

    def test_write_batch(self):
        singer.write_batch('users', '/tmp/users0001.jsonl')


class TestParsingNumbers(unittest.TestCase):
    def create_record(self, value):
        raw = '{"type": "RECORD", "stream": "test", "record": {"value": ' + value + '}}'
        parsed = singer.parse_message(raw)
        return parsed.record['value']

    def test_parse_int_zero(self):
        value = self.create_record('0')
        self.assertEqual(type(value), int)
        self.assertEqual(value, 0)

    def test_parse_regular_decimal(self):
        value = self.create_record('3.14')
        self.assertEqual(3.14, value)

    def test_parse_large_decimal(self):
        value = self.create_record('9999999999999999.9999')
        self.assertEqual(9999999999999999.9999, value)

    def test_parse_small_decimal(self):
        value = self.create_record('-9999999999999999.9999')
        self.assertEqual(-9999999999999999.9999, value)

    def test_parse_absurdly_large_decimal(self):
        value_str = '9' * 1024 + '.' + '9' * 1024
        with self.assertRaises(orjson.JSONDecodeError):
            self.create_record(value_str)

    def test_parse_absurdly_large_int(self):
        value_str = '9' * 1024
        with self.assertRaises(orjson.JSONDecodeError):
            self.create_record(value_str)

    def test_parse_bulk_decs(self):
        value_strs = [
            '-9999999999999999.9999999999999999999999',
            '0',
            '9999999999999999.9999999999999999999999',
            '-7187498962233394.3739812942138415666763',
            '9273972760690975.2044306442955715221042',
            '29515565286974.1188802122612813004366',
            '9176089101347578.2596296292040288441238',
            '-8416853039392703.306423225471199148379',
            '1285266411314091.3002668125515694162268',
            '6051872750342125.3812886238958681227336',
            '-1132031605459408.5571559429308939781468',
            '-6387836755056303.0038029604189860431045',
            '4526059300505414'
        ]
        for value_str in value_strs:
            value = self.create_record(value_str)
            self.assertEqual(float(value_str), value)

    def test_format_message(self):
        record_message = singer.RecordMessage(
            record={'name': 'foo'},
            stream='users')

        self.assertEqual(b'{"type":"RECORD","stream":"users","record":{"name":"foo"}}',
                         singer.format_message(record_message))

        self.assertEqual(b'{"type":"RECORD","stream":"users","record":{"name":"foo"}}',
                         singer.format_message(record_message, option=0))

        self.assertEqual(b'{"type":"RECORD","stream":"users","record":{"name":"foo"}}\n',
                         singer.format_message(record_message, option=orjson.OPT_APPEND_NEWLINE))


if __name__ == '__main__':
    unittest.main()
