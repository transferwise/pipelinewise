venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-twilio/venv/bin/ruff check singer-connectors/tap-twilio/tap_twilio/

test:
	. venv/bin/activate ;\
	pytest tests/unit
