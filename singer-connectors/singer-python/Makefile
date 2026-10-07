venv:
	python3 -m venv venv
	venv/bin/python -m pip install --upgrade pip setuptools wheel
	venv/bin/python -m pip install -e .[test]

lint:
	cd ../.. && singer-connectors/singer-python/venv/bin/ruff check \
		singer-connectors/singer-python/singer/ singer-connectors/singer-python/tests/

unit_test:
	venv/bin/python -m pytest --doctest-modules --cov=singer --cov-fail-under=76 -v singer tests
