FROM python:3.7.4-buster

RUN apt-get -qq update && \
    pip install --upgrade pip

COPY . /app
RUN cd /app && ./install.sh --acceptlicenses --nousage --notestextras

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["pipelinewise"]