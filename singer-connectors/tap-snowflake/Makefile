venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

format:
	. ./venv/bin/activate ;\
	find tap_snowflake tests -type f -name '*.py' | xargs unify --check-only

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile pylintrc tap_snowflake/

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration/ -vvx --cov tap_snowflake
