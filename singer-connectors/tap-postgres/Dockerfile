FROM docker.io/bitnami/postgresql:12

USER root

RUN apt-get update \
    && apt-get -y install git build-essential \
    && git clone --depth 1 --branch wal2json_2_3 https://github.com/eulerto/wal2json.git \
    && cd /wal2json \
    && make && make install \
    && cd / \
    && rm -rf wal2json \
    && rm -r /var/lib/apt/lists /var/cache/apt/archives

USER 1001
