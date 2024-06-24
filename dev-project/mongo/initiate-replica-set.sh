#!/usr/bin/env bash
set -e

echo
echo 'INITIALIZING MONGODB REPLICASET'
echo

mongosh "mongodb://$TAP_MONGODB_ROOT_USER:$TAP_MONGODB_ROOT_PASSWORD@$TAP_MONGODB_HOST:$TAP_MONGODB_PORT/?tls=true&tlsAllowInvalidCertificates=true&authSource=admin&directConnection=true"<<EOF
rs.initiate({_id: "rs0", members: [{_id: 0, host: '$TAP_MONGODB_HOST:$TAP_MONGODB_PORT'}]});
EOF
