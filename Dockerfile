FROM python:3.12-slim-trixie

ARG connectors=all
ARG MONGODB_TOOLS_VERSION=100.18.0
ARG MONGODB_TOOLS_SHA256=a65b3104c87a6a0b9bf15fb748763af8b58c51c60f770e85c885d96cbb28ddda

RUN apt-get -qq update \
    && apt-get -qqy --no-install-recommends install \
        apt-utils \
        gettext-base \
        make \
        mbuffer \
        wget \
        tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && pip install -U --no-cache-dir pip

# MongoDB does not publish Debian 13 ARM64 database tools.
RUN wget -q \
        "https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian13-x86_64-${MONGODB_TOOLS_VERSION}.deb" \
        -O /tmp/mongodb-database-tools.deb \
    && echo "${MONGODB_TOOLS_SHA256}  /tmp/mongodb-database-tools.deb" | sha256sum -c - \
    && apt-get -qq update \
    && apt-get -qqy --no-install-recommends install /tmp/mongodb-database-tools.deb \
    && rm -f /tmp/mongodb-database-tools.deb \
    && rm -rf /var/lib/apt/lists/*

COPY singer-connectors/ /app/singer-connectors/
COPY Makefile /app

RUN echo "setup connectors" \
    && cd /app \
    && if [ "$connectors" = "all" ]; then make all_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" != "all" ] && [ "$connectors" != "none" ] && [ -n "$connectors" ]; then make connectors -e "pw_connector=$connectors" -e pw_acceptlicenses=y; fi

COPY . /app

RUN echo "setup pipelinewise" \
    && cd /app \
    && make pipelinewise_no_test_extras -e pw_acceptlicenses=y\
    && make fastsync-yugabyte_no_test_extras -e pw_acceptlicenses=y\
    && ln -s /root/.pipelinewise /app/.pipelinewise

ENTRYPOINT ["/app/entrypoint.sh"]
