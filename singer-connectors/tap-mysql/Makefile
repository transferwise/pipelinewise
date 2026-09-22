venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-mysql/venv/bin/ruff check \
		singer-connectors/tap-mysql/tap_mysql/ singer-connectors/tap-mysql/tests/unit/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_mysql --cov-fail-under=47 -v tests/unit $(extra_args)

unit_test_cov: unit_test

integration_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_mysql -v tests/integration $(extra_args)
