FROM mongo:5.0.26-focal

# Generate root CA, server certificate (mongodb.pem), and replica.key for MongoDB SSL
RUN openssl genrsa -out /etc/ssl/rootCA.key 4096 && \
	openssl req -x509 -new -nodes -key /etc/ssl/rootCA.key -sha256 -days 3650 -out /etc/ssl/rootCA.pem -subj "/CN=MongoRootCA" && \
	openssl genrsa -out /etc/ssl/server.key 4096 && \
	openssl req -new -key /etc/ssl/server.key -out /etc/ssl/server.csr -subj "/CN=localhost" && \
	openssl x509 -req -in /etc/ssl/server.csr -CA /etc/ssl/rootCA.pem -CAkey /etc/ssl/rootCA.key -CAcreateserial -out /etc/ssl/server.crt -days 3650 -sha256 && \
	cat /etc/ssl/server.key /etc/ssl/server.crt > /etc/ssl/mongodb.pem && \
	openssl rand -out /etc/ssl/replica.key -base64 756

RUN chown mongodb:root /etc/ssl/mongodb.pem && chmod 400 /etc/ssl/mongodb.pem
RUN chown mongodb:root /etc/ssl/rootCA.key && chmod 400 /etc/ssl/rootCA.key
RUN chown mongodb:root /etc/ssl/replica.key && chmod 400 /etc/ssl/replica.key

COPY create-pipelinewise-user.sh /docker-entrypoint-initdb.d/create-pipelinewise-user.sh
