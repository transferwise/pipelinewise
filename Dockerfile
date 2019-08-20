FROM python:3.7.4-buster

RUN apt-get -qq update && \
    pip install --upgrade pip

COPY . /app

RUN groupadd --gid 3434 pipelinewise && \
    useradd --uid 3434 --gid pipelinewise --shell /bin/bash --home-dir /app pipelinewise && \
    chown -R pipelinewise:pipelinewise /app

USER pipelinewise
RUN cd /app && ./install.sh --acceptlicenses --nousage --notestextras

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["pipelinewise"]