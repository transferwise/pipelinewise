venv:
	python3 -m venv venv
	venv/bin/python -m pip install --upgrade pip setuptools wheel
	venv/bin/python -m pip install -e ../singer-python -e .[test]

lint:
	cd ../.. && singer-connectors/transform-field/venv/bin/ruff check \
		singer-connectors/transform-field/transform_field/ \
		singer-connectors/transform-field/tests/unit/

unit_test:
	. venv/bin/activate && python -m pytest -v tests/unit

integration_test:
	. venv/bin/activate && python -m pytest -v tests/integration
