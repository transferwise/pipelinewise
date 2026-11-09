# Snowflake FLOAT row checksums

## Intended behavior

Compare `canonical(expected_target_value(source))` with
`canonical(actual_target_value)` for SQL sources and Snowflake native FLOAT or
managed Iceberg v3 DOUBLE columns. Adjacent finite binary64 values after the
replication cast must remain distinct. No tolerance or decimal-digit rounding
is allowed. The existing aggregate checksum remains probabilistic.

Live source and target metadata select the conversion in [comparison.py](comparison.py).
An existing FLOAT column stays floating point for comparison even when current
creation rules would choose NUMBER or text. PostgreSQL targets and unrelated
unsupported comparisons retain their previous behavior.

## Expected source conversions

| Source declaration | Expected target conversion | Replication evidence |
| --- | --- | --- |
| PostgreSQL/YugabyteDB REAL and DOUBLE PRECISION | Parse the server's float text as binary64; REAL is not direct binary32 promotion. | [FastSync PostgreSQL](../fastsync/commons/tap_postgres.py), [YugabyteDB](../fastsync/commons/tap_yugabyte.py), [Singer SELECT](../../singer-connectors/tap-postgres/tap_postgres/db.py), [WAL](../../singer-connectors/tap-postgres/tap_postgres/sync_strategies/logical_replication.py) |
| MySQL/MariaDB FLOAT | Promote the stored binary32 exactly using `column * 1E0`, matching binary binlog extraction. | [FastSync MySQL](../fastsync/commons/tap_mysql.py), [Singer SELECT](../../singer-connectors/tap-mysql/tap_mysql/sync_strategies/common.py) |
| MySQL/MariaDB DOUBLE and applicable aliases | Preserve binary64; use the physical type reported by source metadata. | Same MySQL extraction paths |
| NUMERIC/DECIMAL fallback or retained legacy FLOAT | Round to binary64, saturate finite overflow with its sign, and reproduce underflow. Retained numeric keys follow their actual target type. | [Decimal contract](../../singer-connectors/singer-python/singer/decimal_support.py), [target loading](../../singer-connectors/target-snowflake/target_snowflake/db_sync.py), [source casts](../fastsync/commons/source_numeric.py) |

Integer sources retained in FLOAT keys also receive the expected binary64 cast.
Only source expressions receive replication conversions. The actual target
value is never saturated or rounded to a source scale, which would conceal drift.
The implementation is [expected_float_sql](float64.py); it does not import
Singer or FastSync execution code.

## Canonicalization decision

Use read-only SQL arithmetic instead of engine-specific HASH, default float
formatting, or a Snowflake TO_BINARY cast. None provides the required shared
raw-bit contract. [float64.py](float64.py) owns the generated projection stages.

A logarithm proposes a bounded binary exponent. Exact power-of-two scaling
then corrects the estimate at either side of a power boundary. Subnormals use
the minimum normal exponent so scaling preserves their integer significands.
The canonical token contains sign, corrected exponent, and two integer chunks.

Each fractional chunk uses 26 bits; the upper chunk also carries the implicit
leading bit for normal values. Power-of-two arithmetic preserves all 53
significand bits. Convert the chunks through DECIMAL(10,0) before text conversion
to keep integer conversions small and avoid default FLOAT text formatting.
This changes the representation without rounding the compared values.

SQL NULL keeps the existing NULL frame. Both zero signs use one zero token.
All NaNs use one NaN token, distinct from NULL. Positive and negative infinity
each have their own token. Special values are excluded from finite arithmetic.

[engine.py](engine.py) wraps the filtered table in projection stages; aliases
avoid selected user columns. [adapters.py](adapters.py) feeds the final tokens
into the existing ordered per-column MD5 framing and centered integer SUM.
No FLOAT aggregation replaces the checksum. Only aggregate results cross the
database boundary; complete rows are never downloaded for comparison.

Finite normalization must satisfy the corrected range and integer-chunk
invariants. The query also sums invalid rows under an internal result alias.
Any nonzero normalization error count raises an execution error instead of
allowing a passing checksum. This adds no UDF, database object, privilege,
runtime dependency, persisted state, or user configuration.

## Acceptance evidence

- [Unit safeguards](../../tests/units/data_diff/test_float_checksum.py) cover
  conversion selection, live target typing, unchanged exact NUMBER scale,
  unsupported pairs, NULL framing, alias collisions, untouched target values,
  and rejection of normalization errors.
- [Real-engine normalization](../../tests/end_to_end/data_diff/test_postgres_to_snowflake.py),
  `test_float64_projection_on_real_engine`, compares SQL tokens against an
  independent IEEE-754 byte decoder across every exponent and neighboring
  values, fixed random patterns, subnormals, signed zero, and special values.
  CI exercises PostgreSQL, YugabyteDB, MySQL, MariaDB, and Snowflake.
- [Replication E2E](../../tests/end_to_end/test_numeric_replication.py) checks
  actual FullSync, PartialSync, and Singer output before DataDiff comparisons.
  It covers native/v3 tables, REAL text conversion, exact FLOAT promotion,
  decimal midpoint rounding and fallback, retained FLOAT, mixed rows, NULLs,
  empty windows, and independently stored one-ULP target mutations.
  Synthetic normalization tests supplement actual replication evidence.

## Operational limits

Compare settled data at rest under the existing preflight and UTC-window rules.
Replication and DataDiff must use matching float output settings, including
replicas: `extra_float_digits` affects PostgreSQL/YSQL text, and older YSQL
defaults can lose digits. YugabyteDB replication keeps its existing native
Snowflake routes. PostgreSQL WAL needs wal2json 2.6+ for nonfinite values.
Previously rounded values or historical NULL substitutions can fail and need
resynchronization; accepting alternate representations would hide those defects.
See the [operator guide](../../docs/user_guide/data_diff.rst).
