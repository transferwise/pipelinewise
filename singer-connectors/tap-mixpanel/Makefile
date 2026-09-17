venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-mixpanel/venv/bin/ruff check singer-connectors/tap-mixpanel/tap_mixpanel/

unit_test:
	. venv/bin/activate ;\
	pytest tests/unittests
