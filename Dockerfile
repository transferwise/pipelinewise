FROM python:3.7-slim-buster

ARG connectors=all
COPY . /app

RUN apt-get -qq update \
    && apt-get -qqy --no-install-recommends install \
        apt-utils \
        alien \
        libaio1 \
        mongo-tools \
        mbuffer \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && pip install -U --no-cache-dir pip \
    # Install Oracle Instant Client for tap-oracle if its in the connectors list
    && bash -c "if grep -q \"tap-oracle\" <<< \"$connectors\"; then wget https://download.oracle.com/otn_software/linux/instantclient/193000/oracle-instantclient19.3-basiclite-19.3.0.0.0-1.x86_64.rpm -O /app/oracle-instantclient.rpm && alien -i /app/oracle-instantclient.rpm --scripts && rm -rf /app/oracle-instantclient.rpm ; fi" \
    && cd /app \
    && make pipelinewise_no_test_extras -e pw_acceptlicenses=y\
    && if [ "$connectors" = "all" ]; then make all_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" = "default" ]; then make default_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" = "extra" ]; then make extra_connectors -e pw_acceptlicenses=y; fi\
    && if [ "$connectors" != "all" ] && [ "$connectors" != "extra" ] && [ "$connectors" != "default" ] && [ "$connectors" != "none" ] && [ ! -z $connectors ]; then make connectors -e pw_connector=$connectors -e pw_acceptlicenses=y; fi\
    && ln -s /root/.pipelinewise /app/.pipelinewise

ENTRYPOINT ["/app/entrypoint.sh"]
