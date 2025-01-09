echo 'CREATE MONGODB PIPELINEWISE USER'

mongo --tls --tlsAllowInvalidCertificates --tlsAllowInvalidHostnames -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin admin <<EOF
    db.getName();
    db.createUser({
        user: $(jq --arg 'user' $MONGO_USERNAME --null-input '$user'),
        pwd: $(jq --arg 'pwd' $MONGO_PASSWORD --null-input '$pwd'),
        roles: [{ role: 'readWrite', db: "$MONGO_INITDB_DATABASE" }]
    });
EOF
