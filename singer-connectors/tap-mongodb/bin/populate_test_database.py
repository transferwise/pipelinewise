import pymongo # requires dnspython package as well
import sys
import bson
import datetime
import re
import time
import decimal
import string
import random

#------ Local mongo server ------
username = sys.argv[1]
password = sys.argv[2]

host= '0.0.0.0'
auth_source = 'reporting'
ssl = False


client = pymongo.MongoClient(host=host, username=username, password=password,
                             port=27017, authSource=auth_source, ssl=ssl, replicaSet='rs0')

databases = {
    "simple_db": ["simple_coll_1", "simple_coll_2"],
    "datatype_db": ["datatype_coll_1", "datatype_coll_2"],
}


############# Drop all dbs/collections #############
for db_name, colls in databases.items():
    for coll_name in colls:
        print("---- Dropping database: " + db_name + ", collection: " + coll_name + " ----")
        client[db_name][coll_name].drop()

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({
            'int_field': int_value,
            'string_field': random_string_generator(),
            'date_created': datetime.datetime.utcnow()})

    return docs


############# Add simple collections #############
# simple_coll_1 has 50 documents
client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(50))

# simple_coll_2 has 100 documents
client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(100))


############# Add datatype collections #############
pattern = re.compile('.*')
regex = bson.Regex.from_native(pattern)
regex.flags ^= re.UNICODE

datatype_doc = {
    "double_field": 4.3,
    "string_field": "a sample string",
    "object_field" : {
        "obj_field_1_key": "obj_field_1_val",
        "obj_field_2_key": "obj_field_2_val"
    },
    "array_field" : [
        "array_item_1",
        "array_item_2",
        "array_item_3"
    ],
    "binary_data_field" : b"a binary string",
    "object_id_field": bson.objectid.ObjectId(b'123456789123'),
    "boolean_field" : True,
    "date_field" : datetime.datetime.now(),
    "null_field": None,
    "regex_field" : regex,
    "32_bit_integer_field" : 32,
    "timestamp_field" : bson.timestamp.Timestamp(int(time.time()), 1),
    "64_bit_integer_field" : 34359738368,
    "decimal_field" : bson.Decimal128(decimal.Decimal('1.34')),
    "javaScript_field" : bson.code.Code("var x, y, z;"),
    "javaScript_with_scope_field" : bson.code.Code("function incrementX() { x++; }", scope={"x": 1}),
    "min_key_field" : bson.min_key.MinKey,
    "max_key_field" : bson.max_key.MaxKey
}

client["datatype_db"]["datatype_coll_1"].insert_one(datatype_doc)
client["datatype_db"]["datatype_coll_2"].insert_one(datatype_doc)

print("\nPrinting database contents")
for db_name in client.list_database_names():
    if db_name in ['admin', 'config', 'local']:
        continue
    for collection_name in client[db_name].list_collection_names():
        print('\n---- Database: '+ db_name +', Collection: ' + collection_name + " ----")
        for doc in client[db_name][collection_name].find():
            print(doc)
