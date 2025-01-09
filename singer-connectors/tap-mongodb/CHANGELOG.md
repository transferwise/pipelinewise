# Changelog

## 1.5.0
   * Switch to MongoDB server 5.0 compatibility

## 1.4.0
   * Bump `pymongo` from `3.12.*` to `4.7.*`


## 1.3.0
   * Support connection to MongoAtlas using `mongodb+srv` protocol
   * Pin dnspython to `2.1.*`
   * Bump `pymongo` from `3.10.*` to `3.12.*`
   * Bump `tzlocal` from `2.0.*` to `2.1.*`
   * Bump dev and test dependencies


## 1.2.0
Add support for SRV urls.

## 1.1.0

Make 2 LOG_BASED parameters configurable:

* `await_time_ms` would control how long the log_based method would wait for new change streams before stopping, default is 1000ms=1s which is the default anyway in the server.

* `update_buffer_size` would control how many update operation we should keep in the memory before having to make a call to `find` operation to get the documents from the server. The default value is 1, i.e every detected update will be sent to stdout right away.

## 1.0.1
   * Fix case where resume tokens has extra properties that are not json serializable by saving `_data` only.

## 1.0.0
   * This is a fork of [Singer's tap-mongodb version 2.0.0](https://github.com/singer-io/tap-mongodb).
   * Oplog replication has been replaced with [MongoDB ChangeStreams](https://docs.mongodb.com/manual/changeStreams/).
   * Custom logging configuration.
