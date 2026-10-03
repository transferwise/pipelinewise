.PHONY: venv lint unit_test integration_test

venv:
	python3 -m venv venv
	./venv/bin/python -m pip install --upgrade pip setuptools wheel
	./venv/bin/python -m pip install -e .[test]

lint:
	cd ../.. && singer-connectors/transform-field/venv/bin/ruff check \
		singer-connectors/transform-field/transform_field/ singer-connectors/transform-field/tests/unit/ \
		singer-connectors/transform-field/tests/integration/ singer-connectors/transform-field/tests/__init__.py

unit_test:
	./venv/bin/python -m pytest -v tests/unit

integration_test:
	PATH="$(CURDIR)/venv/bin:$$PATH" ./venv/bin/python -m pytest -v tests/integration
