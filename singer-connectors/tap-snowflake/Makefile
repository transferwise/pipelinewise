venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-snowflake/venv/bin/ruff check singer-connectors/tap-snowflake/tap_snowflake/

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration/ -vvx --cov tap_snowflake
