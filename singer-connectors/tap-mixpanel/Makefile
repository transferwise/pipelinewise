venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. venv/bin/activate ;\
    pylint tap_mixpanel -d C,W,unexpected-keyword-arg,duplicate-code,too-many-arguments,too-many-locals,too-many-nested-blocks,too-many-statements,too-many-branches,no-else-return,inconsistent-return-statements,no-else-raise,useless-object-inheritance,no-self-argument,raising-non-exception,no-member

unit_test:
	. venv/bin/activate ;\
	pytest tests/unittests
