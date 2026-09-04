COV_OPTIONS ?=

venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc tap_yugabyte/

start_db:
	docker-compose up -d

unit_test:
	. ./venv/bin/activate ;\
	coverage run --data-file=.coverage.unit --source=tap_yugabyte -m pytest -v tests/unit ;\

unit_test_cov: unit_test
	. ./venv/bin/activate ;\
	coverage report --data-file=.coverage.unit --fail-under=65

integration_test:
	. ./venv/bin/activate ;\
	. ./tests/integration/env ;\
	coverage run --data-file=.coverage.integration --source=tap_yugabyte -m pytest -v tests/integration ;\

integration_test_cov: integration_test
	. ./venv/bin/activate ;\
	coverage report --data-file=.coverage.integration --fail-under=0

total_cov:
	. ./venv/bin/activate ;\
	coverage combine ;\
	coverage report --fail-under=0
