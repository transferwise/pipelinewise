"""Validation and placement of the tap-level native-to-Iceberg conversion setting."""

import pytest

from pipelinewise import cli
from pipelinewise.cli.config import Config
from pipelinewise.cli.errors import InvalidConfigException
from tests.units.cli.test_config import _table_format_target


ICEBERG = {'target_table_format': 'iceberg', 'iceberg_version': 3}


def _tap(**settings):
    return {
        'id': 'test_tap',
        'name': 'Test tap',
        'type': 'tap-postgres',
        'db_conn': {},
        'target': 'test_target',
        'schemas': [],
        **settings,
    }


@pytest.mark.parametrize(
    'settings',
    [
        {**ICEBERG, 'native_to_iceberg_conversion': {'enabled': True}},
        {**ICEBERG, 'native_to_iceberg_conversion': {'enabled': True, 'warehouse': 'CONVERSION_WH'}},
        {**ICEBERG, 'type': 'tap-mysql', 'native_to_iceberg_conversion': {'enabled': True}},
        {
            **ICEBERG,
            'type': 'tap-salesforce',
            'data_flattening_max_level': 0,
            'native_to_iceberg_conversion': {'enabled': True},
        },
        {'native_to_iceberg_conversion': {'enabled': False}},
        {'target_table_format': 'native', 'native_to_iceberg_conversion': {'enabled': False}},
    ],
)
def test_accepts_supported_conversion_settings(settings):
    """Any Iceberg v3 Snowflake tap with flattening level 0 may enable conversion."""
    tap = _tap(**settings)

    Config.validate_target_table_format(tap, _table_format_target())
    Config.validate_native_to_iceberg_conversion(tap, _table_format_target())
    cli.utils.validate(tap, cli.utils.load_schema('tap'))


@pytest.mark.parametrize(
    ('settings', 'message'),
    [
        ({'native_to_iceberg_conversion': {'enabled': True}}, 'requires target_table_format "iceberg"'),
        (
            {'target_table_format': 'native', 'native_to_iceberg_conversion': {'enabled': True}},
            'requires target_table_format "iceberg"',
        ),
        (
            {**ICEBERG, 'type': 'tap-salesforce', 'native_to_iceberg_conversion': {'enabled': True}},
            'requires data_flattening_max_level: 0',
        ),
        (
            {**ICEBERG, 'data_flattening_max_level': True, 'native_to_iceberg_conversion': {'enabled': True}},
            'requires data_flattening_max_level: 0',
        ),
        ({**ICEBERG, 'native_to_iceberg_conversion': True}, 'must be an object'),
        ({**ICEBERG, 'native_to_iceberg_conversion': {}}, 'must be an object'),
        ({**ICEBERG, 'native_to_iceberg_conversion': {'enabled': 'yes'}}, 'must be an object'),
        (
            {**ICEBERG, 'native_to_iceberg_conversion': {'enabled': True, 'max_table_size_mb': 10}},
            'must be an object',
        ),
        (
            {**ICEBERG, 'native_to_iceberg_conversion': {'enabled': True, 'warehouse': ' '}},
            'warehouse must be a non-empty string',
        ),
        (
            {**ICEBERG, 'native_to_iceberg_conversion': {'enabled': True, 'warehouse': 7}},
            'warehouse must be a non-empty string',
        ),
    ],
)
def test_rejects_unsupported_conversion_settings(settings, message):
    """Conversion needs explicit Iceberg v3, level-0 columns, and a well-formed setting."""
    tap = _tap(**settings)

    with pytest.raises(InvalidConfigException, match=message):
        Config.validate_native_to_iceberg_conversion(tap, _table_format_target())


def test_rejects_conversion_for_non_snowflake_target():
    """Only target-snowflake has managed Iceberg tables to convert into."""
    target = _table_format_target()
    target['type'] = 'target-postgres'

    with pytest.raises(InvalidConfigException, match='a target-snowflake target'):
        Config.validate_native_to_iceberg_conversion(
            _tap(native_to_iceberg_conversion={'enabled': True}),
            target,
        )


@pytest.mark.parametrize(
    ('kind', 'placement', 'expected'),
    [
        ('Target', 'root', 'target root'),
        ('Target', 'db_conn', 'target db_conn'),
        ('Tap', 'db_conn', 'tap db_conn'),
    ],
)
def test_rejects_misplaced_conversion_settings(kind, placement, expected):
    """The setting belongs to the tap root, never a shared target or a connection."""
    connector = _table_format_target() if kind == 'Target' else _tap(**ICEBERG)
    if placement == 'root':
        connector['native_to_iceberg_conversion'] = {'enabled': True}
    else:
        connector['db_conn']['native_to_iceberg_conversion'] = {'enabled': True}

    with pytest.raises(InvalidConfigException, match=f'cannot set native_to_iceberg_conversion at {expected}'):
        Config.validate_native_to_iceberg_conversion_placement(connector, kind)


def test_tap_root_setting_is_allowed():
    """The tap root is the supported placement."""
    Config.validate_native_to_iceberg_conversion_placement(
        _tap(native_to_iceberg_conversion={'enabled': True}),
        'Tap',
    )


def test_schema_rejects_wrongly_typed_settings():
    """The tap schema checks the setting's value types."""
    with pytest.raises(InvalidConfigException):
        cli.utils.validate(
            _tap(**ICEBERG, native_to_iceberg_conversion={'enabled': 'yes'}),
            cli.utils.load_schema('tap'),
        )


def test_setting_reaches_main_config_but_not_the_target(tmp_path):
    """run_tap reads the setting from the main config; target-snowflake never sees it."""
    config = Config(str(tmp_path))
    conversion = {'enabled': True, 'warehouse': 'CONVERSION_WH'}
    tap = _tap(**ICEBERG, native_to_iceberg_conversion=conversion)
    target = _table_format_target()
    target['taps'] = [tap]
    config.targets = {target['id']: target}

    config.save()

    main_config = cli.utils.load_json(str(tmp_path / 'config.json'))
    inheritable = cli.utils.load_json(str(tmp_path / target['id'] / tap['id'] / 'inheritable_config.json'))
    tap_config = cli.utils.load_json(str(tmp_path / target['id'] / tap['id'] / 'config.json'))
    assert main_config['targets'][0]['taps'][0]['native_to_iceberg_conversion'] == conversion
    assert 'native_to_iceberg_conversion' not in inheritable
    assert 'native_to_iceberg_conversion' not in tap_config
