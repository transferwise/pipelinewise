venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint tap_salesforce -d missing-docstring,invalid-name,line-too-long,too-many-locals,too-few-public-methods,fixme,stop-iteration-return,no-else-return,chained-comparison,broad-except
