FROM mongo:5.0.26-focal

COPY --chown=mongodb:root --chmod=400 rootCA.pem /etc/ssl/rootCA.pem
COPY --chown=mongodb:root --chmod=400 mongodb.pem /etc/ssl/mongodb.pem
COPY --chown=mongodb:root --chmod=400 replica.key /etc/ssl/replica.key

COPY create-pipelinewise-user.sh /docker-entrypoint-initdb.d/create-pipelinewise-user.sh
