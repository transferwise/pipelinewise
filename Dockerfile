FROM python:3.7.4-buster

RUN apt-get -qq update && apt-get -qqy install \
        apt-utils \
        alien \
        libaio1 \
    && pip install --upgrade pip

# Oracle Instant Clinet for tap-oracle
ADD https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm /app/dev-project/oracle-instantclient.rpm
RUN alien -i /app/dev-project/oracle-instantclient.rpm --scripts && rm -rf /app/dev-project/oracle-instantclient.rpm

COPY . /app

RUN cd /app \
    && ./install.sh --acceptlicenses --nousage --notestextras \
    && ln -s /root/.pipelinewise /app/dev-project/.pipelinewise

ENTRYPOINT ["/app/dev-project/entrypoint.sh"]
