from unittest import TestCase

from pipelinewise.fastsync.commons.target_postgres import FastSyncTargetPostgres


class FastSyncTargetPostgresMock(FastSyncTargetPostgres):
    """
    Mocked FastSyncTargetPostgres class
    """

    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []

    def query(self, query, params=None):
        self.executed_queries.append(query)


class TestFastSyncTargetPostgres(TestCase):
    """
    Unit tests for fastsync target postgres
    """

    def setUp(self) -> None:
        """Initialise test FastSyncTargetPostgres object"""
        self.postgres = FastSyncTargetPostgresMock(
            connection_config={}, transformation_config={}
        )

    def test_create_schema(self):
        """Validate if create schema queries generated correctly"""
        self.postgres.create_schema('new_schema')
        assert self.postgres.executed_queries == [
            'CREATE SCHEMA IF NOT EXISTS new_schema'
        ]

    def test_drop_table(self):
        """Validate if drop table queries generated correctly"""
        self.postgres.drop_table('test_schema', 'test_table')
        self.postgres.drop_table('test_schema', 'test_table', is_temporary=True)
        self.postgres.drop_table('test_schema', 'UPPERCASE_TABLE')
        self.postgres.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        self.postgres.drop_table('test_schema', 'test table with space')
        self.postgres.drop_table(
            'test_schema', 'test table with space', is_temporary=True
        )
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."test_table"',
            'DROP TABLE IF EXISTS test_schema."test_table_temp"',
            'DROP TABLE IF EXISTS test_schema."uppercase_table"',
            'DROP TABLE IF EXISTS test_schema."uppercase_table_temp"',
            'DROP TABLE IF EXISTS test_schema."test table with space"',
            'DROP TABLE IF EXISTS test_schema."test table with space_temp"',
        ]

    def test_create_table(self):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.create_table(
            target_schema='test_schema',
            table_name='test_table',
            columns=['"id" INTEGER', '"txt" CHARACTER VARYING'],
            primary_key=['"id"'],
        )
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."test_table" ('
            '"id" integer,"txt" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))'
        ]

        # Create table with reserved words in table and column names
        self.postgres.executed_queries = []
        self.postgres.create_table(
            target_schema='test_schema',
            table_name='ORDER',
            columns=[
                '"id" INTEGER',
                '"txt" CHARACTER VARYING',
                '"SELECT" CHARACTER VARYING',
            ],
            primary_key=['"id"'],
        )
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."order" ('
            '"id" integer,"txt" character varying,"select" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))'
        ]

        # Create table with mixed lower and uppercase and space characters
        self.postgres.executed_queries = []
        self.postgres.create_table(
            target_schema='test_schema',
            table_name='TABLE with SPACE',
            columns=['"id" INTEGER', '"column_with space" CHARACTER VARYING'],
            primary_key=['"id"'],
        )
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."table with space" ('
            '"id" integer,"column_with space" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id"))'
        ]

        # Create table with composite primary key
        self.postgres.executed_queries = []
        self.postgres.create_table(
            target_schema='test_schema',
            table_name='TABLE with SPACE',
            columns=[
                '"id" INTEGER',
                '"num" INTEGER',
                '"column_with space" CHARACTER VARYING',
            ],
            primary_key=['"id"', '"num"'],
        )
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."table with space" ('
            '"id" integer,"num" integer,"column_with space" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying'
            ', PRIMARY KEY ("id","num"))'
        ]

        # Create table with no primary key
        self.postgres.executed_queries = []
        self.postgres.create_table(
            target_schema='test_schema',
            table_name='test_table_no_pk',
            columns=['"id" INTEGER', '"txt" CHARACTER VARYING'],
            primary_key=None,
        )
        assert self.postgres.executed_queries == [
            'CREATE TABLE IF NOT EXISTS test_schema."test_table_no_pk" ('
            '"id" integer,"txt" character varying,'
            '_sdc_extracted_at timestamp without time zone,'
            '_sdc_batched_at timestamp without time zone,'
            '_sdc_deleted_at character varying)'
        ]

    def test_grant_select_on_table(self):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(
            target_schema='test_schema',
            table_name='test_table',
            role='test_role',
            is_temporary=False,
        )
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."test_table" TO GROUP test_role'
        ]

        # GRANT table with reserved word in table and column names in temp table
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(
            target_schema='test_schema',
            table_name='full',
            role='test_role',
            is_temporary=False,
        )
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."full" TO GROUP test_role'
        ]

        # GRANT table with with space and uppercase in table name and s3 key
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_table(
            target_schema='test_schema',
            table_name='table with SPACE and UPPERCASE',
            role='test_role',
            is_temporary=False,
        )
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON test_schema."table with space and uppercase" TO GROUP test_role'
        ]

    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.postgres.executed_queries = []
        self.postgres.grant_usage_on_schema(
            target_schema='test_schema', role='test_role'
        )
        assert self.postgres.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO GROUP test_role'
        ]

    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.postgres.executed_queries = []
        self.postgres.grant_select_on_schema(
            target_schema='test_schema', role='test_role'
        )
        assert self.postgres.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO GROUP test_role'
        ]

    def test_swap_tables(self):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        self.postgres.executed_queries = []
        self.postgres.swap_tables(schema='test_schema', table_name='test_table')
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."test_table"',
            'ALTER TABLE test_schema."test_table_temp" RENAME TO "test_table"',
        ]

        # Swap tables with reserved word in table and column names in temp table
        self.postgres.executed_queries = []
        self.postgres.swap_tables(schema='test_schema', table_name='full')
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."full"',
            'ALTER TABLE test_schema."full_temp" RENAME TO "full"',
        ]

        # Swap tables with with space and uppercase in table name
        self.postgres.executed_queries = []
        self.postgres.swap_tables(
            schema='test_schema', table_name='table with SPACE and UPPERCASE'
        )
        assert self.postgres.executed_queries == [
            'DROP TABLE IF EXISTS test_schema."table with space and uppercase"',
            'ALTER TABLE test_schema."table with space and uppercase_temp" '
            'RENAME TO "table with space and uppercase"',
        ]

    def test_obfuscate_columns_case1(self):
        """
        Test obfuscation where given transformations are emtpy
        Test should pass with no executed queries
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.postgres.transformation_config = {}

        self.postgres.obfuscate_columns(target_schema, table_name)
        self.assertFalse(self.postgres.executed_queries)

    def test_obfuscate_columns_case2(self):
        """
        Test obfuscation where given transformations has an unsupported transformation type
        Test should fail
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.postgres.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'RANDOM',
                }
            ]
        }

        with self.assertRaises(ValueError):
            self.postgres.obfuscate_columns(target_schema, table_name)

        self.assertFalse(self.postgres.executed_queries)

    def test_obfuscate_columns_case3(self):
        """
        Test obfuscation where given transformations have no conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.postgres.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_1',
                    'tap_stream_name': 'public-my_table',
                    'type': 'SET-NULL',
                },
                {
                    'field_id': 'col_2',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-HIDDEN',
                },
                {
                    'field_id': 'col_3',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-DATE',
                },
                {
                    'field_id': 'col_4',
                    'tap_stream_name': 'public-my_table',
                    'safe_field_id': '"col_4"',
                    'type': 'MASK-NUMBER',
                },
                {
                    'field_id': 'col_5',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH',
                },
                {
                    'field_id': 'col_6',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH-SKIP-FIRST-5',
                },
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-STRING-SKIP-ENDS-3',
                },
            ]
        }

        self.postgres.obfuscate_columns(target_schema, table_name)

        self.assertListEqual(
            self.postgres.executed_queries,
            [
                'UPDATE "my_schema"."my_table" SET '
                '"col_1" = NULL, '
                '"col_2" = \'hidden\', '
                '"col_3" = MAKE_TIMESTAMP(DATE_PART(\'year\', "col_3")::int, 1, 1, DATE_PART(\'hour\', "col_3")::int, '
                'DATE_PART(\'minute\', "col_3")::int, DATE_PART(\'second\', "col_3")::double precision), '
                '"col_4" = 0, '
                '"col_5" = ENCODE(DIGEST("col_5", \'sha256\'), \'hex\'), '
                '"col_6" = CONCAT(SUBSTRING("col_6", 1, 5), '
                'ENCODE(DIGEST(SUBSTRING("col_6", 5 + 1), \'sha256\'), \'hex\')), '
                '"col_7" = CASE WHEN LENGTH("col_7") > 2 * 3 THEN '
                'CONCAT(SUBSTRING("col_7", 1, 3), REPEAT(\'*\', LENGTH("col_7")-(2 * 3)), '
                'SUBSTRING("col_7", LENGTH("col_7")-3+1, 3)) '
                'ELSE REPEAT(\'*\', LENGTH("col_7")) END;'
            ],
        )

    def test_obfuscate_columns_case4(self):
        """
        Test obfuscation where given transformations have conditions
        Test should pass
        """
        target_schema = 'my_schema'
        table_name = 'public.my_table'

        self.postgres.transformation_config = {
            'transformations': [
                {
                    'field_id': 'col_1',
                    'tap_stream_name': 'public-my_table',
                    'type': 'SET-NULL',
                },
                {
                    'field_id': 'col_2',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-HIDDEN',
                    'when': [
                        {'column': 'col_4', 'safe_column': '"col_4"', 'equals': None},
                        {
                            'column': 'col_1',
                        },
                    ],
                },
                {
                    'field_id': 'col_3',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-DATE',
                    'when': [{'column': 'col_5', 'equals': 'some_value'}],
                },
                {
                    'field_id': 'col_4',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-NUMBER',
                },
                {
                    'field_id': 'col_5',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH',
                },
                {
                    'field_id': 'col_6',
                    'tap_stream_name': 'public-my_table',
                    'type': 'HASH-SKIP-FIRST-5',
                    'when': [
                        {'column': 'col_1', 'equals': 30},
                        {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                    ],
                },
                {
                    'field_id': 'col_7',
                    'tap_stream_name': 'public-my_table',
                    'type': 'MASK-STRING-SKIP-ENDS-3',
                    'when': [
                        {'column': 'col_1', 'equals': 30},
                        {'column': 'col_2', 'regex_match': r'[0-9]{3}\.[0-9]{3}'},
                        {'column': 'col_4', 'equals': None},
                    ],
                },
            ]
        }

        self.postgres.obfuscate_columns(target_schema, table_name, is_temporary=True)

        self.assertListEqual(
            self.postgres.executed_queries,
            [
                'UPDATE "my_schema"."my_table_temp" SET "col_2" = \'hidden\' WHERE ("col_4" IS NULL);',
                'UPDATE "my_schema"."my_table_temp" SET '
                '"col_3" = MAKE_TIMESTAMP(DATE_PART(\'year\', "col_3")::int, 1, 1, DATE_PART(\'hour\', "col_3")::int, '
                'DATE_PART(\'minute\', "col_3")::int, DATE_PART(\'second\', "col_3")::double precision) '
                'WHERE ("col_5" = \'some_value\');',
                'UPDATE "my_schema"."my_table_temp" SET '
                '"col_6" = CONCAT(SUBSTRING("col_6", 1, 5), '
                'ENCODE(DIGEST(SUBSTRING("col_6", 5 + 1), \'sha256\'), \'hex\')) WHERE ("col_1" = 30) AND '
                '("col_2" ~ \'[0-9]{3}\.[0-9]{3}\');',  # pylint: disable=W1401  # noqa: W605
                'UPDATE "my_schema"."my_table_temp" SET '
                '"col_7" = CASE WHEN LENGTH("col_7") > 2 * 3 THEN '
                'CONCAT(SUBSTRING("col_7", 1, 3), REPEAT(\'*\', LENGTH("col_7")-(2 * 3)), '
                'SUBSTRING("col_7", LENGTH("col_7")-3+1, 3)) '
                'ELSE REPEAT(\'*\', LENGTH("col_7")) END WHERE ("col_1" = 30) AND '
                '("col_2" ~ \'[0-9]{3}\.[0-9]{3}\') AND ("col_4" IS NULL);',  # pylint: disable=W1401  # noqa: W605
                'UPDATE "my_schema"."my_table_temp" SET "col_1" = NULL, '
                '"col_4" = 0, "col_5" = ENCODE(DIGEST("col_5", \'sha256\'), \'hex\');',
            ],
        )
