VENV_DIR = ./venv
ZOOKEEPER_CLIENT_PORT = 2181
KAFKA_PORT = 29092
SCHEMA_REGISTRY_PORT = 8081

.run_pytest_unit:
	@$(VENV_DIR)/bin/pytest --verbose --cov=tap_kafka --cov-fail-under=80 --cov-report term-missing tests/unit

.run_pytest_integration:
	@TAP_KAFKA_BOOTSTRAP_SERVERS=localhost:${KAFKA_PORT} $(VENV_DIR)/bin/pytest --verbose --cov=tap_kafka --cov-fail-under=80 --cov-report term-missing tests/integration

.run_pytest_all:
	@TAP_KAFKA_BOOTSTRAP_SERVERS=localhost:${KAFKA_PORT} $(VENV_DIR)/bin/pytest --verbose --cov=tap_kafka --cov-fail-under=80 --cov-report term-missing tests/

virtual_env:
	@echo "Making Virtual Environment in $(VENV_DIR)..."
	@python3 -m venv $(VENV_DIR)
	@echo "Installing requirements..."
	@. $(VENV_DIR)/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

start_containers: clean_containers
	@docker-compose up -d
	@echo "Waiting for containers..."
	@docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z zookeeper ${ZOOKEEPER_CLIENT_PORT}; do sleep 1; echo 'Waiting for Zookeeper to come up...'; done"
	@docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z kafka ${KAFKA_PORT}; do sleep 1; echo 'Waiting for Kafka to come up...'; done"
	@docker run --rm --network 'pipelinewise_tap_kafka_network' busybox /bin/sh -c "until nc -z schema_registry ${SCHEMA_REGISTRY_PORT}; do sleep 1; echo 'Waiting for Schema Registry to come up...'; done"

clean_containers:
	@echo "Killing and removing containers..."
	@docker-compose kill
	@docker-compose rm -f

clean_virtual_env:
	@echo "Removing Virtual Environment at $(VENV_DIR)..."
	@rm -rf $(VENV_DIR)

clean: clean_containers clean_virtual_env

lint: virtual_env
	@$(VENV_DIR)/bin/pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code

unit_test: virtual_env .run_pytest_unit

integration_test: virtual_env start_containers .run_pytest_integration clean_containers

all_test: virtual_env start_containers .run_pytest_all clean_containers
