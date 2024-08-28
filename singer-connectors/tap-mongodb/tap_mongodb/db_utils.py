"""
List of helper functions for DB related procceses only
"""

import singer

from typing import Dict, List
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from singer import metadata

LOGGER = singer.get_logger('tap_mongodb')

IGNORE_DBS = ['system', 'local', 'config']
ROLES_WITHOUT_FIND_PRIVILEGES = {
    'dbAdmin',
    'userAdmin',
    'clusterAdmin',
    'clusterManager',
    'clusterMonitor',
    'hostManager',
    'restore'
}
ROLES_WITH_FIND_PRIVILEGES = {
    'read',
    'readWrite',
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'dbOwner',
    'backup',
    'root'
}
ROLES_WITH_ALL_DB_FIND_PRIVILEGES = {
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'root'
}

def get_roles_with_find_privs(database: Database, user: Dict) -> List[Dict]:
    """
    Finds and returns all the user's roles that have find privileges.
    User is dictionary in the form:
     {
         '_id': <auth_db>.<user>,
         'db': <auth_db>,
         'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
         'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
                   {'db': 'local', 'role': 'read'}],
         'user': <user>,
         'userId': <userId>
     }
    Args:
        database: MongoDB Database instance
        user: db user dictionary

    Returns: list of roles

    """
    roles = []

    for role in user.get('roles', []):
        if role.get('role') in ROLES_WITHOUT_FIND_PRIVILEGES:
            continue

        role_name = role['role']

        # roles with find privileges
        if role_name in ROLES_WITH_FIND_PRIVILEGES and role.get('db'):
            roles.append(role)

        # for custom roles, get the "sub-roles"
        else:
            role_info_list = database.command({'rolesInfo': {'role': role_name, 'db': database.name}})
            role_info = [r for r in role_info_list.get('roles', []) if r['role'] == role_name]

            if len(role_info) != 1:
                continue

            roles.extend([sub_role for sub_role in role_info[0].get('roles', [])
                          if sub_role.get('role') in ROLES_WITH_FIND_PRIVILEGES and sub_role.get('db')])

    return roles


def get_roles(database: Database, db_user: str) -> List[Dict]:
    """
    Get all user's roles with find privileges if user exists
    Args:
        database: MongoDB DB instance
        db_user: DB user name to get roles for

    Returns: List of roles found

    """

    # usersInfo Command  returns object in shape:
    # {
    #   < some_other_keys >
    #   'users': [
    #                {
    #                    '_id': < auth_db >. < user >,
    #                    'db': < auth_db >,
    #                    'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
    #                     'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
    #                               {'db': 'local', 'role': 'read'}],
    #                     'user': < user >,
    #                     'userId': < userId >
    #                 }
    #           ]
    # }
    user_info = database.command({'usersInfo': db_user})

    users = [u for u in user_info.get('users') if u.get('user') == db_user]
    if len(users) != 1:
        LOGGER.warning('Could not find any users for %s', db_user)
        return []

    return get_roles_with_find_privs(database, users[0])


def get_databases(client: MongoClient, config: Dict) -> List[str]:
    """
    Get all the databases in the cluster that the user roles can read from
    Args:
        client: MongoDB client instance
        config: DB config

    Returns: List of db names

    """
    roles = get_roles(client[config['auth_database']], config['user'])
    LOGGER.info('Roles: %s', roles)

    can_read_all = len([role for role in roles if role['role'] in ROLES_WITH_ALL_DB_FIND_PRIVILEGES]) > 0

    if can_read_all:
        db_names = [d for d in client.list_database_names() if d not in IGNORE_DBS]
    else:
        db_names = [role['db'] for role in roles if role['db'] not in IGNORE_DBS]

    LOGGER.info('Databases: %s', db_names)

    return db_names


def produce_collection_schema(collection: Collection) -> Dict:
    """
    Generate a schema/catalog from the collection details for discovery mode
    Args:
        collection: stream Collection

    Returns: collection catalog

    """
    collection_name = collection.name
    collection_db_name = collection.database.name

    is_view = collection.options().get('viewOn') is not None

    mdata = {}
    mdata = metadata.write(mdata, (), 'table-key-properties', ['_id'])
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.estimated_document_count())
    mdata = metadata.write(mdata, (), 'is-view', is_view)

    # write valid-replication-key metadata by finding fields that have indexes on them.
    # cannot get indexes for views -- NB: This means no key-based incremental for views?
    if not is_view:
        valid_replication_keys = []
        coll_indexes = collection.index_information()
        # index_information() returns a map of index_name -> index_information
        for _, index_info in coll_indexes.items():
            # we don't support compound indexes
            if len(index_info.get('key')) == 1:
                index_field_info = index_info.get('key')[0]
                # index_field_info is a tuple of (field_name, sort_direction)
                if index_field_info:
                    valid_replication_keys.append(index_field_info[0])

        if valid_replication_keys:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)

    return {
        'table_name': collection_name,
        'stream': collection_name,
        'metadata': metadata.to_list(mdata),
        'tap_stream_id': f"{collection_db_name}-{collection_name}",
        'schema': {
            'type': 'object',
            'properties': {
                "_id": {
                    "type": ["string", "null"]
                },
                "document": {
                    "type": [
                        "object",
                        "array",
                        "string",
                        "null"
                    ]
                },
                "_sdc_deleted_at": {
                    "type": [
                        "string",
                        "null"
                    ]
                },
            },
        }
    }
