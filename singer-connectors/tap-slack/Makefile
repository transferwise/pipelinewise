venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint tap_slack -d C,W,unexpected-keyword-arg,duplicate-code,too-many-arguments,too-many-locals,too-many-nested-blocks,useless-object-inheritance,no-self-argument,raising-non-exception,no-member

unit_test:
	. ./venv/bin/activate ;\
	pytest tests --cov tap_slack
