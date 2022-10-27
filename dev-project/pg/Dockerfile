FROM debezium/postgres:12-alpine

RUN apk add --no-cache --virtual .debezium-build-deps gcc clang llvm git make musl-dev pkgconf \
    && git clone --depth 1 --branch wal2json_2_3 https://github.com/eulerto/wal2json.git \
    && cd /wal2json \
    && make && make install \
    && cd / \
    && rm -rf wal2json \
    && apk del .debezium-build-deps

