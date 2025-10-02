FROM quay.io/debezium/postgres:12-alpine

COPY --chown=postgres:postgres server.crt /var/lib/postgresql/server.crt
COPY --chown=postgres:postgres --chmod=600 server.key /var/lib/postgresql/server.key

RUN apk update
RUN apk add --no-cache --virtual .debezium-build-deps gcc clang19 llvm19 git make musl-dev pkgconf \
    && git clone --depth 1 --branch wal2json_2_3 https://github.com/eulerto/wal2json.git \
    && cd /wal2json \
    && make && make install \
    && cd / \
    && rm -rf wal2json \
    && apk del .debezium-build-deps

