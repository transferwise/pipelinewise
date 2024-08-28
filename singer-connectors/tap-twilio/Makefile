venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. venv/bin/activate ;\
	pylint tap_twilio --disable 'broad-except,chained-comparison,empty-docstring,fixme,invalid-name,line-too-long,missing-class-docstring,missing-function-docstring,missing-module-docstring,no-else-raise,no-else-return,too-few-public-methods,too-many-arguments,too-many-branches,too-many-lines,too-many-locals,ungrouped-imports'

test:
	. venv/bin/activate ;\
	pytest tests/unit