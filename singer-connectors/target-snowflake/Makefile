venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile pylintrc target_snowflake/

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit -vv --cov target_snowflake --cov-fail-under=67

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration/ -vvx --cov target_snowflake --cov-fail-under=86
