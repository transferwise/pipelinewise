venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-salesforce/venv/bin/ruff check singer-connectors/tap-salesforce/tap_salesforce/
