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
	nosetests -c .noserc --cover-min-percentage=47 tests/unit $(extra_args)

integration_test:
	. ./venv/bin/activate ;\
	nosetests -c .noserc tests/integration $(extra_args)
