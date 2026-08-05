venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	. ./venv/bin/activate ;\
	ruff format --check . ;\
	ruff check .

format:
	. ./venv/bin/activate ;\
	ruff format .

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit -vv --cov target_snowflake --cov-fail-under=67

unit_test_cov: unit_test

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration/ -vvx --cov target_snowflake --cov-fail-under=86
