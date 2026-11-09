"""Read-only SQL normalization of the expected replicated binary64 value.

Power-of-two scaling is exact in binary64. Each finite value is represented by
its sign, binary exponent and integer significand, preserving every finite ULP.
This avoids relying on a database's decimal FLOAT printer or binary casts.
"""

from collections.abc import Callable


_FLOAT64_ZERO_MIDPOINT = f'{5 ** 1075}e-1075'
_CHUNK_FACTOR = 2 ** 26
FLOAT_NORMALIZATION_ERRORS = '_ppw_float_normalization_errors'


def expected_float_sql(name: str, conversion: str) -> str:
    """Apply the replication cast only to a source expression."""
    if conversion == 'postgres_float_text':
        return f'({name})::text::double precision'
    if conversion == 'postgres_numeric':
        # Match the documented decimal FLOAT fallback, including PostgreSQL's
        # otherwise error-producing overflow and zero-underflow casts.
        limit = '1.7976931348623157e308'
        name = f'({name})::numeric'
        return (
            f"CASE WHEN {name}::text IN ('NaN', 'Infinity', '-Infinity') THEN {name}::double precision "
            f'WHEN {name} > {limit}::numeric THEN {limit}::double precision '
            f'WHEN {name} < -{limit}::numeric THEN -{limit}::double precision '
            f'WHEN abs({name}) <= {_FLOAT64_ZERO_MIDPOINT}::numeric THEN 0::double precision '
            f'WHEN abs({name}) < 5e-324::numeric '
            f"THEN sign({name})::double precision * '5e-324'::double precision "
            f'ELSE {name}::double precision END'
        )
    if conversion == 'mysql_numeric':
        # A DOUBLE operand forces exact promotion of MySQL/MariaDB binary32
        # FLOAT, matching both Singer extraction and FastSync exports.
        return f'({name} * 1E0)'
    return name


def float64_projection(
    columns: list[dict], quote: Callable[[str], str], dialect: str, reserved_names: tuple[str, ...] = (),
) -> tuple[list[list[str]], list[dict]]:
    """Return SQL projection stages and descriptors using their final tokens.

    Callers initially project only the selected columns. Internal names avoid
    those columns, so user identifiers cannot shadow intermediate expressions.
    Every stage has linear size in the number of FLOAT columns.
    """
    float_columns = [(index, column) for index, column in enumerate(columns)
                     if column['checksum_kind'] == 'float64']
    if not float_columns:
        return [], columns
    occupied = {column['name'].lower() for column in columns} | {name.lower() for name in reserved_names}
    prefix = '_ppw_f64_'
    while any(name.startswith(prefix) for name in occupied):
        prefix = f'_{prefix}'
    has_mysql_decimal = any(column.get('checksum_float_conversion') == 'mysql_decimal' for _, column in float_columns)
    stages = [[] for _ in range(7 if has_mysql_decimal else 6)]
    descriptors = [dict(column) for column in columns]
    for index, column in float_columns:
        stem = f'{prefix}{index}_'
        if column.get('checksum_float_conversion') == 'mysql_decimal':
            decimal_stages = _mysql_decimal_projection(column['name'], stem, quote)
            for stage, additions in zip(stages, decimal_stages):
                stage.extend(additions)
            descriptors[index]['checksum_float_token'] = quote(f'{stem}token')
            descriptors[index]['checksum_float_invalid'] = quote(f'{stem}invalid')
            continue
        value = quote(f'{stem}value')
        name = quote(column['name'])
        expression = expected_float_sql(name, column.get('checksum_float_conversion', 'identity'))
        stages[0].append(f'{expression} AS {value}')
        finite = f'NULLIF({value}, 0)'
        if dialect != 'mysql':
            cast = '::FLOAT' if dialect == 'snowflake' else '::double precision'
            finite = f"CASE WHEN {value} IN ('NaN'{cast}, 'inf'{cast}, '-inf'{cast}) THEN NULL ELSE {finite} END"
        magnitude = quote(f'{stem}magnitude')
        stages[1].append(f'ABS({finite}) AS {magnitude}')
        e0 = quote(f'{stem}estimate')
        logarithm = f'LN({magnitude}) / LN({_float_literal(2.0, dialect)})'
        stages[2].append(f'GREATEST(-1022, LEAST(1023, FLOOR({logarithm}))) AS {e0}')
        r0 = quote(f'{stem}scaled')
        stages[3].append(f'{magnitude} / POWER({_float_literal(2.0, dialect)}, {e0}) AS {r0}')
        r = quote(f'{stem}significand')
        e = quote(f'{stem}exponent')
        two = _float_literal(2.0, dialect)
        stages[4].extend([
            f'CASE WHEN {r0} >= 2 THEN {r0} / {two} '
            f'WHEN {r0} < 1 AND {e0} > -1022 THEN {r0} * {two} ELSE {r0} END AS {r}',
            f'{e0} + CASE WHEN {r0} >= 2 THEN 1 '
            f'WHEN {r0} < 1 AND {e0} > -1022 THEN -1 ELSE 0 END AS {e}',
        ])
        factor = _float_literal(float(_CHUNK_FACTOR), dialect)
        upper = f'FLOOR({r} * {factor})'
        lower = f'({r} * {factor} - {upper}) * {factor}'
        text_type = 'CHAR' if dialect == 'mysql' else 'VARCHAR'
        upper_text = f'CAST(CAST({upper} AS DECIMAL(10, 0)) AS {text_type})'
        lower_text = f'CAST(CAST({lower} AS DECIMAL(10, 0)) AS {text_type})'
        token = (
            f"CONCAT(CASE WHEN {value} < 0 THEN '-' ELSE '+' END, "
            f"CAST({e} AS {text_type}), ':', {upper_text}, ':', {lower_text})"
        )
        cases = f"CASE WHEN {value} IS NULL THEN NULL WHEN {value} = 0 THEN '0' "
        if dialect != 'mysql':
            cases += (
                f"WHEN {value} = 'NaN'{cast} THEN 'nan' "
                f"WHEN {value} = 'inf'{cast} THEN '+inf' "
                f"WHEN {value} = '-inf'{cast} THEN '-inf' "
            )
        # LOG only proposes an exponent. Report an aggregate error if exact
        # power-of-two arithmetic cannot verify its corrected result.
        invalid = quote(f'{stem}invalid')
        stages[5].append(
            f'({r} >= 2 OR ({r} < 1 AND {e} > -1022) OR {r} <= 0 '
            f'OR {lower} <> FLOOR({lower})) AS {invalid}'
        )
        normalized = quote(f'{stem}token')
        stages[5].append(f'{cases}ELSE {token} END AS {normalized}')
        descriptors[index]['checksum_float_token'] = normalized
        descriptors[index]['checksum_float_invalid'] = invalid
    return stages, descriptors


def _mysql_exact_power_of_two(exponent: str) -> str:
    """Build an exact DECIMAL power; casting a large DOUBLE power loses digits."""
    return (
        f'(CASE WHEN {exponent} >= 128 THEN 340282366920938463463374607431768211456 ELSE 1 END '
        f'* CASE WHEN MOD({exponent}, 128) >= 64 THEN 18446744073709551616 ELSE 1 END '
        f'* CASE WHEN MOD({exponent}, 64) >= 32 THEN 4294967296 ELSE 1 END '
        f'* CAST(POWER(2, MOD({exponent}, 32)) AS DECIMAL(10, 0)))'
    )


def _mysql_scale_decimal(value: str, exponent: str) -> str:
    """Scale in small steps so MySQL preserves the complete fractional operand."""
    factors = [
        f'CASE WHEN {exponent} >= {threshold} THEN 4294967296 ELSE 1 END'
        for threshold in (32, 64, 96, 128, 160)
    ]
    factors.append(f'CAST(POWER(2, MOD({exponent}, 32)) AS DECIMAL(10, 0))')
    return f"({value} * {' * '.join(factors)})"


def _mysql_decimal_projection(name: str, stem: str, quote: Callable[[str], str]) -> list[list[str]]:
    """Round exact MySQL/MariaDB decimals to binary64 using quotient/remainder.

    Their decimal-to-DOUBLE parsers can misround immediately beside a binary
    midpoint, including when parsing decimal text. Native DECIMAL arithmetic
    preserves the discarded remainder and implements nearest-even explicitly.
    The supported DECIMAL domain needs at most 215-bit integer powers of two;
    scaled significands and exact remainders fit its 65-digit arithmetic.
    """
    name = quote(name)
    a, estimate, scale, ratio, exponent, factor, integer, remainder, denominator, rounded = (
        quote(f'{stem}{suffix}') for suffix in (
            'magnitude', 'estimate', 'scale', 'ratio', 'exponent', 'factor',
            'integer', 'remainder', 'denominator', 'rounded',
        )
    )
    magnitude = f'ABS(NULLIF({name}, 0))'
    correction = f'CASE WHEN {ratio} < 1 THEN -1 WHEN {ratio} >= 2 THEN 1 ELSE 0 END'
    binary_exponent = f'({estimate} + {correction})'
    scaled = _mysql_scale_decimal(a, f'(52 - {exponent})')
    exponent_ratio = _mysql_scale_decimal(a, f'(-{estimate})')
    stages = [
        [f'{magnitude} AS {a}', f'FLOOR(LN({magnitude} * 1E0) / LN(2E0)) AS {estimate}'],
        [f'{_mysql_exact_power_of_two(f"ABS({estimate})")} AS {scale}'],
        [f'CASE WHEN {estimate} >= 0 THEN {a} DIV {scale} ELSE {exponent_ratio} END AS {ratio}'],
        [f'{binary_exponent} AS {exponent}',
         f'{_mysql_exact_power_of_two(f"ABS({binary_exponent} - 52)")} AS {factor}'],
        [f'CASE WHEN {exponent} <= 52 THEN FLOOR({scaled}) ELSE {a} DIV {factor} END AS {integer}',
         f'CASE WHEN {exponent} <= 52 THEN {scaled} - FLOOR({scaled}) '
         f'ELSE MOD({a}, {factor}) END AS {remainder}',
         f'CASE WHEN {exponent} <= 52 THEN 1 ELSE {factor} END AS {denominator}'],
        [f'{integer} + CASE WHEN {remainder} * 2 > {denominator} '
         f'OR ({remainder} * 2 = {denominator} AND MOD({integer}, 2) = 1) THEN 1 ELSE 0 END AS {rounded}'],
    ]
    significand = f'CASE WHEN {rounded} = 9007199254740992 THEN 4503599627370496 ELSE {rounded} END'
    normalized_exponent = f'{exponent} + CASE WHEN {rounded} = 9007199254740992 THEN 1 ELSE 0 END'
    token = (
        f"CASE WHEN {name} IS NULL THEN NULL WHEN {name} = 0 THEN '0' "
        f"ELSE CONCAT(CASE WHEN {name} < 0 THEN '-' ELSE '+' END, "
        f"CAST(CAST({normalized_exponent} AS SIGNED) AS CHAR), ':', "
        f"CAST(({significand}) DIV 67108864 AS CHAR), ':', CAST(MOD(({significand}), 67108864) AS CHAR)) END"
    )
    stages.append([
        f'{token} AS {quote(f"{stem}token")}',
        f'({rounded} < 4503599627370496 OR {rounded} > 9007199254740992) AS {quote(f"{stem}invalid")}',
    ])
    return stages


def _float_literal(value: float, dialect: str) -> str:
    """Force DOUBLE arithmetic even for integer-valued powers of two."""
    text = repr(value)
    if dialect == 'mysql':
        return text if 'e' in text else f'{text}E0'
    cast = 'FLOAT' if dialect == 'snowflake' else 'double precision'
    return f"'{text}'::{cast}"
