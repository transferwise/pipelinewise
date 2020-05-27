# Initialize a replica set
# the initialization is not going to resolve the host db_mongo_source, and we end up with 127.0.0.1 which prevent another docker container from connection to this replica set
# solution is re-configure the replica set, link to issue https://github.com/yougov/mongo-connector/issues/391
mongo -u $MONGO_INITDB_ROOT_USERNAME -p $MONGO_INITDB_ROOT_PASSWORD --authenticationDatabase admin $MONGO_INITDB_DATABASE <<EOF
  db.getName()
  rs.initiate({
    _id: "rs0",
    members: [
      {_id: 0, host: "127.0.0.1:27017"}
    ]
  });

  cfg=rs.conf();
  cfg.members[0].host="$MONGO_REPLICA_HOST:27017";
  rs.reconfig(cfg);

  use admin;

  db.createUser({
		user: $(jq --arg 'user' $MONGO_USERNAME --null-input '$user'),
		pwd: $(jq --arg 'pwd' $MONGO_PASSWORD --null-input '$pwd'),
		roles: [{ role: 'readWrite', db: "$MONGO_INITDB_DATABASE" }]
	});
EOF
