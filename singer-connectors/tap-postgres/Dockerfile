FROM postgres:14-bookworm

# pgoutput is built into PostgreSQL. Keep wal2json in the PG14 test image so
# integration tests can verify automatic migration of historical slots.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-14-wal2json \
    && rm -rf /var/lib/apt/lists/*
