"""Source SELECTs retain FastSync masking semantics before exporting rows."""

import pytest

from pipelinewise.fastsync.commons.transform_utils import TransformationType
from pipelinewise.fastsync.commons.source_transformations import (
    UnsupportedSourceTransformation,
    compile_source_select,
    requires_regex_support,
    validate_bookmark_column,
    validate_source_transformation_config,
)


def column(name='secret', data_type='text', target_type='VARCHAR(134217728)', **kwargs):
    return dict(column_name=name, data_type=data_type, target_type=target_type, safe_sql_value=f'"{name}"', **kwargs)


def rule(kind='HASH', field='secret', **kwargs):
    return dict(tap_stream_name='public-events', field_id=field, type=kind, **kwargs)


def compile_rules(rules, columns=None, dialect='postgres', **kwargs):
    return compile_source_select(
        'public.events', '"public"."events"', kwargs.get('where', ''),
        columns or [column()], {'transformations': rules}, dialect, kwargs.get('iceberg_version'),
    )


@pytest.mark.parametrize('invalid_rules', [
    [rule(), rule(field='SECRET', when=[])],
    [rule(when='not-a-list')],
    [rule(when=[None])],
    [rule(when=[{'column': 'secret'}])],
    [rule(when=[{'column': '', 'equals': 'a'}])],
    [rule(when=[{'column': 'secret', 'equals': 0, 'regex_match': '.*'}])],
    [rule(when=[{'column': 'secret', 'equals': 'a', 'unknown': True}])],
    [rule(when=[{'column': 'secret', 'field_path': 'nested', 'equals': 'a'}])],
    [rule(when=[{'column': 'secret', 'equals': {'nested': 'value'}}])],
    [rule(when=[{'column': 'secret', 'equals': '\0'}])],
    [rule(when=[{'column': 'secret', 'equals': float('inf')}])],
    [rule(when=[{'column': 'secret', 'equals': float('nan')}])],
    [rule(when=[{'column': 'secret', 'equals': r'C:\path'}])],
    [rule(when=[{'column': 'secret', 'regex_match': r'\d+'}])],
    [rule(when=[{'column': 'secret', 'regex_match': 'a^b'}])],
    [rule(when=[{'column': 'secret', 'regex_match': '[a&&b]'}])],
    [rule(field_paths=['nested'])],
    [rule('UNKNOWN')],
])
def test_configuration_validation_rejects_unsupported_rules_without_metadata(invalid_rules):
    with pytest.raises(UnsupportedSourceTransformation):
        validate_source_transformation_config('public.events', {'transformations': invalid_rules})


@pytest.mark.parametrize('equals', [None, '', 'a\nb', 0, False, 1.25])
def test_configuration_validation_defers_column_type_checks(equals):
    validate_source_transformation_config('public.events', {
        'transformations': [rule('MASK-NUMBER', when=[{'column': 'unknown_until_discovery', 'equals': equals}])],
    })


def test_regex_capability_is_requested_only_for_matching_condition_rules():
    config = {'transformations': [rule(when=[{'column': 'secret', 'regex_match': '[0-9]+'}])]}
    assert requires_regex_support('public.events', config)
    assert not requires_regex_support('public.other', config)
    assert not requires_regex_support('public.events', None)
    assert not requires_regex_support('public.events', {'transformations': [rule()]})


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_without_matching_rules_keeps_existing_export(dialect):
    assert compile_rules([], dialect=dialect) is None
    assert compile_rules([dict(rule(), tap_stream_name='public-other')], dialect=dialect) is None


@pytest.mark.parametrize('dialect,hash_sql', [
    ('postgres', 'sha256(convert_to('), ('mysql', 'SHA2('), ('mariadb', 'SHA2('),
])
def test_hash_is_in_source_select_with_original_alias(dialect, hash_sql):
    sql = compile_rules([rule()], dialect=dialect)
    assert hash_sql in sql
    assert sql.count(' AS ') >= 3
    assert 'UPDATE ' not in sql
    assert 'digest(' not in sql


def test_boundary_and_existing_normalization_stay_in_innermost_select():
    cols = [dict(column(), safe_sql_value='replace("secret", chr(1), \'\') AS "secret"')]
    sql = compile_rules([rule()], cols, where='WHERE "id" >= 17')
    assert 'replace("secret", chr(1), \'\')' in sql
    assert 'AS "secret")' not in sql
    assert 'FROM "public"."events" WHERE "id" >= 17)' in sql


def test_conditional_rules_precede_all_unconditional_rules():
    columns = [column('secret'), column('amount', 'integer', 'NUMBER')]
    sql = compile_rules([
        rule(),
        rule('MASK-NUMBER', 'amount', when=[{'column': 'secret', 'equals': 'A'}]),
        rule('SET-NULL', 'amount', when=[{'column': 'amount', 'equals': 0}]),
    ], columns)
    assert sql.index('sha256') < sql.index('"amount" = 0') < sql.index("decode('41'")
    assert sql.count('AS ppw_transform') == 3


def test_unconditional_rules_share_one_projection():
    sql = compile_rules([rule(), rule('HASH', 'other')], [column(), column('other')])
    assert sql.count('SELECT ') == 2
    assert sql.count('sha256(') == 2


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('kind,expected', [
    ('SET-NULL', 'NULL AS'), ('MASK-HIDDEN', '68696464656e'),
    ('HASH-SKIP-FIRST-9', ', 10)'), ('MASK-STRING-SKIP-ENDS-9', '> 18'),
])
def test_supported_string_transformations(kind, expected, dialect):
    sql = compile_rules([rule(kind)], dialect=dialect)
    assert expected in sql
    assert 'COALESCE' not in sql


def test_postgres_hash_skip_preserves_null_with_strict_concatenation():
    sql = compile_rules([rule('HASH-SKIP-FIRST-1')])
    assert ' || ' in sql
    assert 'CONCAT(' not in sql
    assert 'SUBSTRING("secret", 2)' in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_mask_ends_counts_characters_and_includes_exact_boundary(dialect):
    sql = compile_rules([rule('MASK-STRING-SKIP-ENDS-2')], dialect=dialect)
    assert 'CHAR_LENGTH(' in sql
    assert '> 4 THEN' in sql
    assert "ELSE REPEAT('*', CHAR_LENGTH(" in sql


@pytest.mark.parametrize('dialect,expected', [
    ('postgres', '::time - TIME'), ('mysql', 'DAYOFYEAR(`secret`) - 1'), ('mariadb', 'DAYOFYEAR(`secret`) - 1'),
])
def test_mask_date_keeps_clock_and_fractional_seconds(dialect, expected):
    sql = compile_rules([rule('MASK-DATE')], [column(data_type='timestamp', target_type='TIMESTAMP_NTZ')], dialect)
    assert expected in sql
    assert 'MAKEDATE' not in sql


def test_numeric_mask_keeps_numeric_zero():
    sql = compile_rules([rule('MASK-NUMBER')], [column(data_type='integer', target_type='NUMBER')])
    assert 'SELECT 0 AS "secret"' in sql


def test_postgres_fixed_width_text_uses_copy_output_function_before_hash():
    sql = compile_rules([rule()], [column(data_type='character', character_maximum_length=7)])
    assert 'format(\'%s\', "secret")' in sql
    assert 'IS NOT DISTINCT FROM NULL' in sql
    assert 'rpad(' not in sql
    assert '::text' not in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_condition_literals_cannot_inject_sql(dialect):
    payload = "' OR 1=1 -- trailing\n"
    sql = compile_rules([rule(when=[{'column': 'secret', 'equals': payload}])], dialect=dialect)
    assert payload not in sql
    assert payload.encode().hex() in sql
    assert ' COLLATE "C" = ' in sql if dialect == 'postgres' else ' AS BINARY)' in sql


@pytest.mark.parametrize('value,expected', [(None, 'IS NULL'), ('', "decode(''"), (0, ' = 0)'), (False, ' = FALSE)')])
def test_falsy_equals_values_are_not_discarded(value, expected):
    target_type = 'BOOLEAN' if value is False else 'NUMBER' if value == 0 else 'VARCHAR(134217728)'
    cols = [column(), column('condition', target_type=target_type)]
    sql = compile_rules([rule(when=[{'column': 'condition', 'equals': value}])], cols)
    assert expected in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_regex_whole_string_case_and_newline_contract(dialect):
    sql = compile_rules([rule(when=[{'column': 'secret', 'regex_match': '[A-Z].*|[801]'}])], dialect=dialect)
    end = r'\Z' if dialect == 'postgres' else r'\z'
    flags = '' if dialect == 'postgres' else '(?-x)'
    expected = (flags + r'\A(?:[A-Z][^' + '\n' + ']*|[801])' + end).encode().hex()
    assert expected in sql
    assert 'COLLATE "C" ~ ' in sql if dialect == 'postgres' else 'COLLATE utf8mb4_bin REGEXP ' in sql


@pytest.mark.parametrize('pattern', [r'\d+', '(?i)secret', r'(a)\1', '(?=a)a', 'a*?', 'a{256}', '[[:alpha:]]', '['])
def test_unproven_regex_syntax_fails_closed(pattern):
    with pytest.raises(UnsupportedSourceTransformation):
        compile_rules([rule(when=[{'column': 'secret', 'regex_match': pattern}])])


@pytest.mark.parametrize('rules,columns', [
    ([rule(), rule()], [column()]),
    ([rule(field='missing')], [column()]),
    ([rule()], [column(), column('SECRET')]),
    ([rule('HASH')], [column(data_type='integer', target_type='NUMBER')]),
    ([rule('MASK-NUMBER')], [column()]),
    ([rule('MASK-DATE')], [column()]),
    ([rule('UNKNOWN')], [column()]),
    ([rule(field_paths=['nested'])], [column()]),
    ([rule(field_path='nested')], [column()]),
    ([rule(when=[{'column': 'secret', 'field_path': 'nested', 'equals': 'a'}])], [column()]),
    ([rule(when=[{'column': 'secret'}])], [column()]),
    ([rule(when=[{'column': 'secret', 'equals': 'a', 'regex_match': 'a'}])], [column()]),
    ([rule(when=[{'column': 'secret', 'equals': 0}])], [column()]),
    ([rule()], [dict(column(), safe_sql_value='"secret" AS "other"')]),
])
def test_unsupported_configuration_fails_before_export(rules, columns):
    with pytest.raises(UnsupportedSourceTransformation):
        compile_rules(rules, columns)


def test_exact_quoted_aliases_are_preserved():
    cols = [dict(column('say"hi'), safe_sql_value='"say""hi" AS "say""hi"')]
    sql = compile_rules([rule(field='say"hi')], cols)
    assert 'AS "say""hi"' in sql


def test_type_alias_inside_cast_is_not_stripped():
    sql = compile_rules([rule()], [dict(column(), safe_sql_value='CAST("secret" AS TEXT)')])
    assert 'CAST("secret" AS TEXT)' in sql


def test_set_null_can_cover_non_string_types():
    sql = compile_rules([rule('SET-NULL')], [column(data_type='jsonb', target_type='VARIANT')])
    assert 'SELECT NULL AS "secret"' in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_integer_regex_matches_snowflake_decimal_text_conversion(dialect):
    cols = [
        column(), column('number', data_type='integer', target_type='NUMBER', column_type='int(10) unsigned zerofill'),
    ]
    sql = compile_rules([rule(when=[{'column': 'number', 'regex_match': '[801]'}])], cols, dialect)
    expected = '("number")::text' if dialect == 'postgres' else 'CAST((`number` + 0) AS CHAR CHARACTER SET utf8mb4)'
    assert expected in sql


def test_untouched_columns_keep_their_original_export_expression():
    cols = [column(), column('amount', data_type='numeric', target_type='FLOAT')]
    sql = compile_rules([rule()], cols)
    assert 'double precision' not in sql
    assert '"amount" AS "amount"' in sql


@pytest.mark.parametrize('config', [None, [], {'transformations': None}, {'transformations': [None]}])
def test_malformed_config_cannot_disable_masking(config):
    with pytest.raises(UnsupportedSourceTransformation):
        compile_source_select('public.events', 'events', '', [column()], config, 'postgres')


@pytest.mark.parametrize('expression', ['"secret" AS "secret"; SELECT 1', '"secret', '("secret"'])
def test_untrusted_metadata_aliases_fail_closed(expression):
    with pytest.raises(UnsupportedSourceTransformation):
        compile_rules([rule()], [dict(column(), safe_sql_value=expression)])


def test_postgres_single_bit_condition_uses_staged_boolean_semantics():
    cols = [column(), column('flag', data_type='bit', target_type='BOOLEAN', character_maximum_length=1)]
    sql = compile_rules([rule(when=[{'column': 'flag', 'equals': True}])], cols)
    assert '(CAST(("flag") AS integer) <> 0)' in sql
    assert '("flag" = TRUE)' in sql


@pytest.mark.parametrize('width', [None, 0, 2])
def test_postgres_ambiguous_bit_width_fails_closed(width):
    cols = [column(), column('flag', data_type='bit', target_type='BOOLEAN', character_maximum_length=width)]
    with pytest.raises(UnsupportedSourceTransformation, match='one-bit'):
        compile_rules([rule(when=[{'column': 'flag', 'equals': True}])], cols)


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('operator,value', [('equals', r'C:\path'), ('regex_match', r'\.'), ('regex_match', r'\\.')])
def test_ambiguous_legacy_backslash_conditions_fail_closed(dialect, operator, value):
    with pytest.raises(UnsupportedSourceTransformation, match='legacy Snowflake literal'):
        compile_rules([rule(when=[{'column': 'secret', operator: value}])], dialect=dialect)


def test_literal_regex_punctuation_uses_portable_character_class():
    sql = compile_rules([rule(when=[{'column': 'secret', 'regex_match': '[.]'}])])
    assert (r'\A(?:[.])\Z').encode().hex() in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('pattern', ['[].a]', '[^].a]'])
def test_literal_closing_bracket_regex_classes_fail_closed(dialect, pattern):
    with pytest.raises(UnsupportedSourceTransformation, match='Literal closing brackets'):
        compile_rules([rule(when=[{'column': 'secret', 'regex_match': pattern}])], dialect=dialect)


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_float_conditions_use_export_text_instead_of_promoting_source_float_bits(dialect):
    cols = [column(), column('fraction', data_type='real', target_type='FLOAT')]
    sql = compile_rules([rule(when=[{'column': 'fraction', 'equals': 0.3}])], cols, dialect)
    expected = '("fraction")::text::double precision' if dialect == 'postgres' else (
        '(CAST(("fraction") AS CHAR CHARACTER SET utf8mb4) + 0e0)'
    )
    assert expected in sql
    assert '= 0.3)' in sql


@pytest.mark.parametrize('kind', ['SET-NULL', 'HASH', 'MASK-DATE', 'MASK-NUMBER'])
@pytest.mark.parametrize('when', [None, [{'column': 'secret', 'equals': 'condition-that-never-matches'}]])
def test_masked_incremental_key_cannot_leak_through_bookmark(kind, when):
    config = {'transformations': [rule(kind, field='SECRET', when=when)]}
    with pytest.raises(UnsupportedSourceTransformation, match='raw bookmarks are required'):
        validate_bookmark_column('PUBLIC.Events', 'secret', config)


@pytest.mark.parametrize('config,key', [
    (None, 'secret'), ({'transformations': [rule()]}, None),
    ({'transformations': []}, 'secret'), ({'transformations': [rule()]}, 'id'),
    ({'transformations': [dict(rule(), tap_stream_name='public-other')]}, 'secret'),
])
def test_untransformed_incremental_bookmarks_remain_supported(config, key):
    assert validate_bookmark_column('public.events', key, config) is None


@pytest.mark.parametrize('config', [
    [], {'transformations': None}, {'transformations': [None]},
    {'transformations': [dict(rule(), field_id=None)]},
    {'transformations': [dict(rule(), when_otherwise='HASH')]},
    {'transformations': [dict(rule(), type=None)]},
    {'transformations': [dict(rule(), type='UNKNOWN')]},
    {'transformations': [dict(rule(), when=False)]},
])
def test_malformed_bookmark_transformation_config_fails_closed(config):
    with pytest.raises(UnsupportedSourceTransformation):
        validate_bookmark_column('public.events', 'id', config)


@pytest.mark.parametrize('dialect', ['mysql', 'mariadb'])
@pytest.mark.parametrize('pattern', ['a b', 'a#b', 'a\tb'])
def test_regex_literal_whitespace_and_hash_ignore_session_extended_mode(dialect, pattern):
    sql = compile_rules([rule(when=[{'column': 'secret', 'regex_match': pattern}])], dialect=dialect)
    assert ('(?-x)' + r'\A(?:' + pattern + r')\z').encode().hex() in sql


@pytest.mark.parametrize('data_type', ['inet', 'cidr', 'character', 'USER-DEFINED'])
def test_postgres_string_fallback_preserves_type_output_and_composite_nulls(data_type):
    sql = compile_rules([rule()], [column(data_type=data_type)])
    assert 'CASE WHEN ("secret") IS NOT DISTINCT FROM NULL THEN NULL ELSE format(\'%s\', "secret") END' in sql
    assert '::text' not in sql
    assert '("secret") IS NULL' not in sql


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('pattern', ['[a&&b]', '[&&]', '[a&&]', '[--b]', '[a--b]'])
def test_regex_character_class_set_operators_fail_closed(dialect, pattern):
    with pytest.raises(UnsupportedSourceTransformation, match='character-class set operators'):
        compile_rules([rule(when=[{'column': 'secret', 'regex_match': pattern}])], dialect=dialect)


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
@pytest.mark.parametrize('pattern', ['a&&b', 'a--b', '[&]&', '[-]-', '[a&b]'])
def test_literal_ampersands_and_hyphens_remain_supported(dialect, pattern):
    sql = compile_rules([rule(when=[{'column': 'secret', 'regex_match': pattern}])], dialect=dialect)
    assert pattern.encode().hex() in sql


@pytest.mark.parametrize('data_type', ['bit varying', 'varbit'])
@pytest.mark.parametrize('value', [101, None])
def test_postgres_bit_string_conditions_use_decimal_export_value(data_type, value):
    cols = [column(), column('bits', data_type=data_type, target_type='NUMBER')]
    sql = compile_rules([rule(when=[{'column': 'bits', 'equals': value}])], cols)
    assert '("bits")::text::numeric(38, 0) AS "bits"' in sql
    assert '("bits" = 101)' in sql if value is not None else '("bits" IS NULL)' in sql
    assert 'COALESCE' not in sql
    assert '::integer' not in sql


@pytest.mark.parametrize('data_type', ['bit varying', 'varbit'])
def test_postgres_conditional_bit_string_mask_keeps_numeric_case_branches(data_type):
    cols = [column(), column('bits', data_type=data_type, target_type='NUMBER')]
    sql = compile_rules([rule('MASK-NUMBER', 'bits', when=[{'column': 'secret', 'equals': 'mask'}])], cols)
    assert '("bits")::text::numeric(38, 0) AS "bits"' in sql
    assert 'THEN 0 ELSE "bits" END AS "bits"' in sql


@pytest.mark.parametrize('data_type', ['bit varying', 'varbit'])
def test_postgres_bit_string_regex_uses_normalized_numeric_text(data_type):
    cols = [column(), column('bits', data_type=data_type, target_type='NUMBER')]
    sql = compile_rules([rule(when=[{'column': 'bits', 'regex_match': '101'}])], cols)
    assert '("bits")::text::numeric(38, 0) AS "bits"' in sql
    assert '("bits")::text COLLATE "C" ~ ' in sql


def test_postgres_unreferenced_bit_string_keeps_existing_projection():
    cols = [column(), column('bits', data_type='bit varying', target_type='NUMBER')]
    sql = compile_rules([rule()], cols)
    assert 'numeric(38, 0)' not in sql
    assert '"bits" AS "bits"' in sql


@pytest.mark.parametrize('kind', [item.value for item in TransformationType])
@pytest.mark.parametrize('target_type,allowed', [
    ('VARCHAR(134217728)', 'text'), ('NUMBER', 'number'), ('DOUBLE', 'number'),
    ('DATE', 'date'), ('TIMESTAMP_NTZ', 'date'), ('TIME', 'null'),
    ('BINARY', 'null'), ('BOOLEAN', 'null'), ('VARIANT', 'null'),
])
def test_every_transformation_has_an_explicit_mapped_type_contract(kind, target_type, allowed, subtests):
    supported = (
        kind == 'SET-NULL'
        or (allowed == 'number' and kind == 'MASK-NUMBER')
        or (allowed == 'date' and kind == 'MASK-DATE')
        or (allowed == 'text' and kind not in {'MASK-NUMBER', 'MASK-DATE'})
    )
    for dialect in ('postgres', 'mysql', 'mariadb'):
        for version in (None, 3):
            with subtests.test(dialect=dialect, iceberg_version=version):
                cols = [column(data_type='text', target_type=target_type)]
                if supported:
                    sql = compile_rules([rule(kind)], cols, dialect, iceberg_version=version)
                    assert 'SELECT ' in sql
                    assert 'AS "secret"' in sql if dialect == 'postgres' else 'AS `secret`' in sql
                else:
                    with pytest.raises(UnsupportedSourceTransformation, match='does not support mapped target type'):
                        compile_rules([rule(kind)], cols, dialect, iceberg_version=version)


@pytest.mark.parametrize('dialect', ['postgres', 'mysql', 'mariadb'])
def test_date_mask_exports_a_date_when_the_existing_mapping_requires_date(dialect):
    sql = compile_rules(
        [rule('MASK-DATE', when=[{'column': 'gate', 'equals': 'mask'}])],
        [column(data_type='date', target_type='DATE'), column('gate')], dialect,
    )
    assert ' AS DATE) AS ' in sql
    assert ' AS DATE) ELSE ' in sql
    assert '::time' not in sql


@pytest.mark.parametrize('alias,kind', [
    ('TEXT', 'HASH'), ('DOUBLE PRECISION', 'MASK-NUMBER'),
    ('NUMERIC(18,2)', 'MASK-NUMBER'), ('DATETIME', 'MASK-DATE'), ('BOOL', 'SET-NULL'),
])
def test_native_ddl_aliases_use_the_same_type_contract(alias, kind):
    assert compile_rules([rule(kind)], [column(target_type=alias)])


@pytest.mark.parametrize('kind,width', [('HASH', 64), ('HASH-SKIP-FIRST-9', 73), ('MASK-HIDDEN', 6)])
def test_text_output_must_fit_the_mapped_type_without_changing_it(kind, width):
    assert compile_rules([rule(kind)], [column(target_type=f'VARCHAR({width})')])
    with pytest.raises(UnsupportedSourceTransformation, match='output requires VARCHAR'):
        compile_rules([rule(kind)], [column(target_type=f'VARCHAR({width - 1})')])


def test_iceberg_uses_existing_version_mapping_instead_of_native_width():
    # The v3 mapper canonicalizes string columns to its full physical width.
    assert compile_rules([rule()], [column(target_type='VARCHAR(1)')], iceberg_version=3)


@pytest.mark.parametrize('field', ['column_name', 'data_type', 'target_type', 'safe_sql_value'])
def test_missing_column_metadata_fails_with_a_controlled_preflight_error(field):
    metadata = column()
    del metadata[field]
    with pytest.raises(UnsupportedSourceTransformation):
        compile_rules([rule()], [metadata])


@pytest.mark.parametrize('target_type', [None, '', 'UNRECOGNIZED', 'VARCHAR(0)'])
def test_set_null_does_not_bypass_unknown_mapped_types(target_type):
    with pytest.raises(UnsupportedSourceTransformation, match='Unsupported mapped target type'):
        compile_rules([rule('SET-NULL')], [column(target_type=target_type)])
