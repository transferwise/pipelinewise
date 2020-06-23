FROM python:3.7.7-slim-buster

RUN apt-get -qq update && apt-get -qqy install \
        apt-utils \
        alien \
        libaio1 \
        mongo-tools \
        mbuffer \
    && pip install --upgrade pip

# Oracle Instant Clinet for tap-oracle
ADD https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm /app/oracle-instantclient.rpm
RUN alien -i /app/oracle-instantclient.rpm --scripts && rm -rf /app/oracle-instantclient.rpm

COPY . /app

RUN cd /app \
    && ./install.sh --connectors=all --acceptlicenses --nousage --notestextras \
    && ln -s /root/.pipelinewise /app/.pipelinewise

ENTRYPOINT ["/app/entrypoint.sh"]
