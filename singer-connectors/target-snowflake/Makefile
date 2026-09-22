venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/target-snowflake/venv/bin/ruff check \
		singer-connectors/target-snowflake/target_snowflake/ singer-connectors/target-snowflake/tests/unit/ \
		singer-connectors/target-snowflake/tests/__init__.py

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit -vv --cov target_snowflake --cov-fail-under=67

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration/ -vvx --cov target_snowflake --cov-fail-under=86
