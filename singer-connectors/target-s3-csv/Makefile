venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

lint:
	cd ../.. && singer-connectors/target-s3-csv/venv/bin/ruff check singer-connectors/target-s3-csv/target_s3_csv/

unit_test:
	. ./venv/bin/activate ;\
	pytest tests/unit --cov target_s3_csv --cov-fail-under=75

integration_test:
	. ./venv/bin/activate ;\
	pytest tests/integration --cov target_s3_csv --cov-fail-under=72
