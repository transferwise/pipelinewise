FROM python:3.7-slim-buster

ARG connectors=default

RUN apt-get -qq update \
    && apt-get -qqy --no-install-recommends install \
        apt-utils \
        alien \
        libaio1 \
        mbuffer \
        wget \
        git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install -U --no-cache-dir pip

# In order to use fastsync with MongoDB Atlas we need version 100+ of mongodump
RUN cd /tmp && \
    wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian10-x86_64-100.5.1.deb && \
    dpkg -i mongodb-database-tools-debian10-x86_64-100.5.1.deb && \
    rm mongodb-database-tools-debian10-x86_64-100.5.1.deb

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
