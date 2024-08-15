venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc tap_zendesk/

unit_test:
	. ./venv/bin/activate ;\
	pytest -v tests --cov tap_zendesk --cov-fail-under=67
