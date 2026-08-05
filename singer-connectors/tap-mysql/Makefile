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
	pytest --cov=tap_mysql --cov-fail-under=47 -v tests/unit $(extra_args)

unit_test_cov: unit_test

integration_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_mysql -v tests/integration $(extra_args)
