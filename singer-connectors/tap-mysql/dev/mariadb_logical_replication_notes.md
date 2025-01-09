# Guide for how to setup MariaDB logical replication

## Create a replica server

1. Create a new Mariadb server by adding the following bits to [docker-compose](../docker-compose.yml):
```yaml
  mariadb_server2:
    container_name: "mariadb_server2"
    image: mariadb:10.6
    environment:
      MYSQL_ROOT_PASSWORD: my-secret-passwd
      MYSQL_USER: replication_user
      MYSQL_PASSWORD: secret123passwd
    ports:
      - "3307:3306"
    command: |
      --server-id=2
      --default-authentication-plugin=mysql_native_password
      --binlog-format=row
    networks:
      - mariadb-cluster
```

2. Freeze all activity on server 1 (primary) and get current binlog file and pos
```mariadb
FLUSH TABLES WITH READ LOCK;

show master status ;
```

Save binlog file and pos somewhere. 

3. Export all objects/data from primary into a file:

 * Log into the container: `docker exec -it mariadb_server1 bash`
 * Export objects/data: `mysqldump -B mydb --master-data --gtid > masterfull.sql -p`
 * Copy export file into your host: `docker cp mariadb_server1:/masterfull.sql .`

2. Import the exported file into server 2
 * Copy exported file into server 2: `docker cp masterfull.sql mariadb_server2:/`
 * Log into the container: `docker exec -it mariadb_server2 bash`
 * Export objects/data: `mysql -p < masterfull.sql`

3. Setup replica 
```mariadb

stop slave;
reset master;

CHANGE MASTER TO
    MASTER_HOST='mariadb_primary',
    MASTER_USER='replication_user',
    MASTER_PASSWORD='secret123passwd',
    MASTER_LOG_FILE='<saved binlog file from step 2>',
    MASTER_LOG_POS=<saved binlog pos from step 2>,
    MASTER_PORT=3306,
    MASTER_CONNECT_RETRY=10
;

SELECT @@gtid_slave_pos; -- check this is set

start slave;

show slave status; -- should say Slave is waiting for master to send events
```
4. Unfreeze activity on primary
```mariadb
UNLOCK TABLES;
```

5. Test replication is working fine by writing some data into an existing table:
```mariadb
Insert into table_x(id) values(1);
```
Check replica has this new row in `table_x`
