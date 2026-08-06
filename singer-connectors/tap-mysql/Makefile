venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc tap_mysql/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_mysql --cov-fail-under=47 -v tests/unit $(extra_args)

unit_test_cov: unit_test

integration_test:
	. ./venv/bin/activate ;\
	pytest --cov=tap_mysql -v tests/integration $(extra_args)
