venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/tap-slack/venv/bin/ruff check singer-connectors/tap-slack/tap_slack/

unit_test:
	. ./venv/bin/activate ;\
	pytest tests --cov tap_slack
