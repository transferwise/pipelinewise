FROM python:3.7.7-slim-buster

RUN apt-get -qq update && apt-get install -y git && \
        apt-get -qqy install \
        apt-utils \
        alien \
        libaio1 \
        mongo-tools \
        mbuffer \
        wget \
    && pip install "setuptools==57.5.0"


ARG connectors=all
COPY . /app

# Install Oracle Instant Client for tap-oracle if its in the connectors list
#RUN bash -c "if grep -q \"tap-oracle\" <<< \"$connectors\"; then wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm -O /app/oracle-instantclient.rpm && alien -i /app/oracle-instantclient.rpm --scripts && rm -rf /app/oracle-instantclient.rpm ; fi"

RUN cd /app \
    && ./exo_install.sh --connectors=$connectors --acceptlicenses --nousage --notestextras \
    && ln -s /root/.pipelinewise /app/.pipelinewise

ENTRYPOINT ["/app/entrypoint.sh"]
