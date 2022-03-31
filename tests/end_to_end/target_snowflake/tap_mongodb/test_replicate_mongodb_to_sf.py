import decimal
import uuid
from datetime import datetime
from random import randint

import bson
from tests.end_to_end.helpers import assertions
from tests.end_to_end.target_snowflake.tap_mongodb import TapMongoDB

TAP_ID = 'mongo_to_sf'
TARGET_ID = 'snowflake'


class TestReplicateMongoDBToSF(TapMongoDB):
    """
    Test replicate MongoDB to Snowflake
    """

    # pylint: disable=arguments-differ
    def setUp(self):
        super().setUp(tap_id=TAP_ID, target_id=TARGET_ID)
        self.mongodb_con = self.e2e_env.get_tap_mongodb_connection()

    def assert_row_counts_equal(self, target_schema, table, count_in_source):
        """
        Check if row counts are equal in source and target
        """

        self.assertEqual(
            count_in_source,
            self.e2e_env.run_query_target_snowflake(
                f'select count(_id) from {target_schema}.{table}'
            )[0][0],
        )

    def assert_columns_exist(self, table):
        """
        Check if every table and column exists in the target
        """

        assertions.assert_cols_in_table(
            self.e2e_env.run_query_target_snowflake,
            f'ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}',
            table,
            [
                '_ID',
                'DOCUMENT',
                '_SDC_EXTRACTED_AT',
                '_SDC_BATCHED_AT',
                '_SDC_DELETED_AT',
            ],
            schema_postfix=self.e2e_env.sf_schema_postfix,
        )

    def test_replicate_mongodb_to_sf(self):
        """
        Test replicate MongoDB to Snowflake
        """

        # Run tap first time - fastsync and singer should be triggered
        assertions.assert_run_tap_success(
            self.tap_id, self.target_id, ['fastsync', 'singer']
        )
        self.assert_columns_exist('listings')
        self.assert_columns_exist('my_collection')
        self.assert_columns_exist('all_datatypes')

        listing_count = self.mongodb_con['listings'].count_documents({})
        my_coll_count = self.mongodb_con['my_collection'].count_documents({})
        all_datatypes_count = self.mongodb_con['all_datatypes'].count_documents({})

        self.assert_row_counts_equal(
            f'ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}',
            'listings',
            listing_count,
        )
        self.assert_row_counts_equal(
            f'ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}',
            'my_collection',
            my_coll_count,
        )
        self.assert_row_counts_equal(
            f'ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}',
            'all_datatypes',
            all_datatypes_count,
        )

        result_insert = self.mongodb_con.my_collection.insert_many(
            [
                {
                    'age': randint(10, 30),
                    'id': 1001,
                    'uuid': uuid.uuid4(),
                    'ts': bson.Timestamp(12030, 500),
                },
                {
                    'date': datetime.utcnow(),
                    'id': 1002,
                    'uuid': uuid.uuid4(),
                    'regex': bson.Regex(r'^[A-Z]\\w\\d{2,6}.*$'),
                },
                {
                    'uuid': uuid.uuid4(),
                    'id': 1003,
                    'decimal': bson.Decimal128(
                        decimal.Decimal('5.64547548425446546546644')
                    ),
                    'nested_json': {
                        'a': 1,
                        'b': 3,
                        'c': {'key': bson.datetime.datetime(2020, 5, 3, 10, 0, 0)},
                    },
                },
            ]
        )
        my_coll_count += len(result_insert.inserted_ids)

        result_del = self.mongodb_con.my_collection.delete_one(
            {'_id': result_insert.inserted_ids[0]}
        )
        my_coll_count -= result_del.deleted_count

        result_update = self.mongodb_con.my_collection.update_many(
            {}, {'$set': {'id': 0}}
        )

        assertions.assert_run_tap_success(self.tap_id, self.target_id, ['singer'])

        self.assertEqual(
            result_update.modified_count,
            self.e2e_env.run_query_target_snowflake(
                f'select count(_id) from ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}.my_collection'
                f' where document:id = 0'
            )[0][0],
        )

        self.assert_row_counts_equal(
            f'ppw_e2e_tap_mongodb{self.e2e_env.sf_schema_postfix}',
            'my_collection',
            my_coll_count,
        )
