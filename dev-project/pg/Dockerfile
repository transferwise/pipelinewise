FROM quay.io/debezium/postgres:16-alpine

COPY --chown=postgres:postgres server.crt /var/lib/postgresql/server.crt
COPY --chown=postgres:postgres --chmod=600 server.key /var/lib/postgresql/server.key

# The base image ships decoderbufs and pgoutput but not wal2json, which
# tap-postgres needs for logical decoding, so it is compiled here.
# clang/llvm must match the major the base image was built against, since PGXS
# hardcodes it as CLANG=clang-21 for the JIT bitcode step.
RUN apk update
RUN apk add --no-cache --virtual .debezium-build-deps gcc clang21 llvm21 git make musl-dev pkgconf \
    && git clone --depth 1 --branch wal2json_2_6 https://github.com/eulerto/wal2json.git \
    && cd /wal2json \
    && make && make install \
    && cd / \
    && rm -rf wal2json \
    && apk del .debezium-build-deps

