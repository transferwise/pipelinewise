venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/target-postgres/venv/bin/ruff check \
		singer-connectors/target-postgres/target_postgres/ singer-connectors/target-postgres/tests/unit/ \
		singer-connectors/target-postgres/tests/integration/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=target_postgres  --cov-fail-under=44 tests/unit -v

integration_test: export TARGET_POSTGRES_PORT ?= 5432
integration_test: export TARGET_POSTGRES_DBNAME ?= target_db
integration_test: export TARGET_POSTGRES_USER ?= my_user
integration_test: export TARGET_POSTGRES_PASSWORD ?= secret
integration_test: export TARGET_POSTGRES_HOST ?= localhost
integration_test: export TARGET_POSTGRES_SCHEMA ?= target_postgres_ci

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration --cov=target_postgres  --cov-fail-under=87 -v
