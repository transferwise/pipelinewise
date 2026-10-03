FROM quay.io/debezium/postgres:16-alpine

COPY --chown=postgres:postgres server.crt /var/lib/postgresql/server.crt
COPY --chown=postgres:postgres --chmod=600 server.key /var/lib/postgresql/server.key

# The base image ships native pgoutput. Compile wal2json as well so development
# and E2E tests can exercise migration of historical PipelineWise slots.
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
