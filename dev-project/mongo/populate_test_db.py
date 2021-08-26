import os
import pymongo  # requires dnspython package as well
import bson
import datetime
import re
import time
import decimal
import string
import random


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def generate_all_datatypes_doc():
    pattern = re.compile('.*')
    regex = bson.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE

    return {
        "double_field": random.randrange(-15, 15)/3,
        "string_field": random_string_generator(100),
        "object_field": {
            "obj_field_1_key": "obj_field_1_val",
            "obj_field_2_key": "obj_field_2_val"
        },
        "array_field": [
            None,
            random.randrange(-1, 1)/5,
            {"k": "v"},
            "array_item",
            bson.Decimal128(decimal.Decimal(f'{random.randrange(-10, 10)/6}')),
        ],
        "binary_data_field": b"a binary string",
        "object_id_field": bson.objectid.ObjectId(),
        "boolean_field": True,
        "date_field": datetime.datetime.now(),
        "null_field": None,
        "regex_field": regex,
        "32_bit_integer_field": 32,
        "timestamp_field": bson.timestamp.Timestamp(int(time.time()), random.randint(0, 100)),
        "64_bit_integer_field": 34359738368,
        "decimal_field": bson.Decimal128(decimal.Decimal(f'{random.randrange(-100, 100)/33}')),
        "javaScript_field": bson.code.Code("var x, y, z;"),
        "javaScript_with_scope_field": bson.code.Code("function incrementX() { x++; }", scope={"x": 1}),
        "min_key_field": bson.min_key.MinKey(),
        "max_key_field": bson.max_key.MaxKey()
    }


def populate_all_datatypes_collection(db_name, col_name, n=100):
    print(f'Populating MongoDB collection {db_name}.{col_name} with {n} documents ...')
    for _ in range(n):
        client[db_name][col_name].insert_one(generate_all_datatypes_doc())


if __name__ == '__main__':
    creds = {
        'host': os.getenv('TAP_MONGODB_HOST'),
        'username': os.getenv('TAP_MONGODB_USER'),
        'password': os.getenv('TAP_MONGODB_PASSWORD'),
        'port': int(os.getenv('TAP_MONGODB_PORT')),
        'authSource': 'admin',
        'ssl': False,
        'replicaSet': 'rs0',
        'connect': True
    }

    client = pymongo.MongoClient(**creds)

    db_name = 'mongo_source_db'

    populate_all_datatypes_collection(db_name, 'all_datatypes')
