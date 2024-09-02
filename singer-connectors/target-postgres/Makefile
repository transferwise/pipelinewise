venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile .pylintrc target_postgres/

unit_test:
	. ./venv/bin/activate ;\
	pytest --cov=target_postgres  --cov-fail-under=44 tests/unit -v

env:
  	export TARGET_POSTGRES_PORT=5432
  	export TARGET_POSTGRES_DBNAME=target_db
  	export TARGET_POSTGRES_USER=my_user
  	export TARGET_POSTGRES_PASSWORD=secret
  	export TARGET_POSTGRES_HOST=localhost
  	export TARGET_POSTGRES_SCHEMA=public

integration_test: env
	. ./venv/bin/activate ;\
	pytest tests/integration --cov=target_postgres  --cov-fail-under=87 -v
