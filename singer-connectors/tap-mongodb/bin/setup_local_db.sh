#!/usr/bin/env bash

# Local DB host, dev and test purposes only
MONGDB_HOST="0.0.0.0" 
MONGDB_PORT=27017
TRY_LOOP=3

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while [ -n "$(python3 -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.connect((\"${host}\", ${port}))" 2>&1)" ]
  do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}


echo "Starting MongoDB docker container ..."
docker-compose down && docker-compose up -d db
sleep 5
echo "Checking if MongoDB is up and ready .."
wait_for_port "MongoDB" "$MONGDB_HOST" "$MONGDB_PORT"

echo "Init ReplicSet"
docker-compose exec db mongo -u mongoAdmin -p Password1 --authenticationDatabase admin --eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "127.0.0.1:27017"}]})'

echo "Adding new user etl..."
docker-compose exec db mongo -u mongoAdmin -p Password1 --authenticationDatabase admin reporting --eval 'db.createUser({user: "etl",pwd: "secret@1", roles:[{ role: "readWrite", db: "simple_db"}, { role: "readWrite", db: "datatype_db"}]})'



