echo 'Initializing MongoDB ReplicaSet'

mongo mongodb://$TAP_MONGODB_HOST:$TAP_MONGODB_PORT/admin?authSource=admin -u $TAP_MONGODB_ROOT_USER -p $TAP_MONGODB_ROOT_PASSWORD <<EOF
  db.getName();
  let cfg = {};
  rs.initiate({
    _id: "rs0",
    members: [
      {_id: 0, host: "$TAP_MONGODB_HOST:$TAP_MONGODB_PORT"}
    ]
  });
EOF