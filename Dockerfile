FROM python:3.8-slim-buster

ARG connectors=all

RUN apt-get -qq update \
    && apt-get -qqy --no-install-recommends install \
        apt-utils \
        alien \
        gnupg \
        libaio1 \
        mbuffer \
        wget \
        git \
        fuse \
    && rm -rf /var/lib/apt/lists/* \
    && pip install -U --no-cache-dir pip

# Add Mongodb ppa
RUN wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - \
    && echo "deb [ arch=amd64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | tee /etc/apt/sources.list.d/mongodb.list \
    && apt-get -qq update \
    && apt-get -qqy --no-install-recommends install \
        mongodb-database-tools \
    && rm -rf /var/lib/apt/lists/*

# Install GCSFuse
ENV GCSFUSE_VERSION 0.39.2
RUN wget https://github.com/GoogleCloudPlatform/gcsfuse/releases/download/v${GCSFUSE_VERSION}/gcsfuse_${GCSFUSE_VERSION}_amd64.deb \
    && dpkg --install gcsfuse_${GCSFUSE_VERSION}_amd64.deb \
    && rm -Rf gcsfuse_${GCSFUSE_VERSION}_amd64.deb

COPY singer-connectors/ /app/singer-connectors/
COPY Makefile /app

RUN echo "setup connectors" \
    # Install Oracle Instant Client for tap-oracle if its in the connectors list
    && bash -c "if grep -q \"tap-oracle\" <<< \"$connectors\"; then wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm -O /app/oracle-instantclient.rpm && alien -i /app/oracle-instantclient.rpm --scripts && rm -rf /app/oracle-instantclient.rpm ; fi" \
    && cd /app \
    && if [ "$connectors" = "all" ]; then make all_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" = "default" ]; then make default_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" = "extra" ]; then make extra_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" != "all" ] && [ "$connectors" != "extra" ] && [ "$connectors" != "default" ] && [ "$connectors" != "none" ] && [ ! -z $connectors ]; then make connectors -e pw_connector=$connectors -e pw_acceptlicenses=y; fi

COPY . /app

RUN echo "setup pipelinewise" \
    && cd /app \
    && make pipelinewise_no_test_extras -e pw_acceptlicenses=y\
    && ln -s /root/.pipelinewise /app/.pipelinewise

ENTRYPOINT ["/app/entrypoint.sh"]
