.. _schema_changes:

Schema changes
==============

Singer taps emit schema messages and targets apply compatible changes before
loading affected records. Target behaviour preserves existing data rather than
silently coercing or deleting it.


Change outcomes
---------------

.. list-table::
   :header-rows: 1
   :widths: 27 35 38
   :width: 100%

   * - Source change
     - Target action
     - Operational consequence
   * - Add column
     - Add a compatible target column.
     - Historical rows normally contain ``NULL`` until explicitly backfilled.
   * - Drop column
     - Retain the target column.
     - Historical values remain queryable; remove it manually only after
       downstream review.
   * - Change data type
     - Rename the old target column with a timestamp suffix and create a new
       column using the new type.
     - Old and new values are split across columns until a resync or downstream
       migration.

Target-specific type mapping and experimental connector behaviour still apply.
Test changes that narrow precision, alter timezone semantics, or change nested
JSON shape before production rollout.


.. _versioning_columns:

Column versioning
-----------------

If ``COLUMN_THREE`` changes from ``INTEGER`` to ``VARCHAR``, the target keeps the
old data in a name such as ``COLUMN_THREE_20260809_1520`` and writes subsequent
records to a new ``COLUMN_THREE`` column.

PipelineWise does not convert historical values into the new type. Queries that
need one logical field must handle both versions until the table is deliberately
resynced or migrated.


Operational procedure
---------------------

Before a planned source change:

1. inspect target type mapping and downstream contracts;
2. test discovery and one representative load;
3. decide whether historical values require conversion;
4. notify consumers of versioned or retained columns; and
5. retain enough source log to recover if the first changed record fails.

After the change, verify the new target schema and representative values. Use
:ref:`resync` only when its source and target cost is acceptable; a resync is not
required merely because an old target column remains.


.. _exact_decimal_mapping:

Decimal mapping
---------------

MariaDB/MySQL ``DECIMAL(p,s)`` or ``NUMERIC(p,s)`` and PostgreSQL
``NUMERIC(p,s)`` or ``DECIMAL(p,s)`` retain their declared precision and scale
where supported by Snowflake native, Snowflake managed Iceberg v3, and PostgreSQL
targets. This applies to Singer and supported FullSync/PartialSync routes. New
columns always use this mapping. Snowflake reports ``NUMERIC`` as its equivalent
``NUMBER`` type.
Approximate ``FLOAT``, ``DOUBLE``, and ``REAL`` columns keep their existing mapping.
Other sources and destination combinations keep their existing behavior.
PostgreSQL numeric primary keys use canonical text on Snowflake so a ``NaN``
key remains representable; other supported decimal columns use the mapping
below.

Run ``import_config`` after upgrading to refresh the generated tap configuration
and catalogs before replication resumes. Decimal values and decimal replication
bookmarks travel as exact strings within the Singer protocol. Their schema
contains the original source precision and scale. Targets choose a compatible
column type from those dimensions. Generic string and number schemas are unaffected.
Quote precise decimal values in YAML transformation conditions so YAML does not
parse them as floating-point numbers.

Snowflake uses the original declaration when precision is at most 38 and scale
is between zero and the lesser of precision and 37. Otherwise, PipelineWise
first tries a numeric declaration that retains the values. For example,
``NUMERIC(10,-2)`` becomes ``NUMERIC(12,0)``, and ``NUMERIC(2,4)`` becomes
``NUMERIC(4,4)``. Larger definitions and unconstrained PostgreSQL ``NUMERIC``
use ``FLOAT`` in native tables and ``DOUBLE`` in Iceberg tables. Warnings identify
the column and chosen type. Both mappings use double precision. Finite values beyond
FLOAT range are clamped to the largest finite value with the same sign. Very
small values can round to zero. Explicit infinities and NaN remain floating-point
special values on this fallback path.

PostgreSQL also permits NaN in bounded numeric columns. PostgreSQL targets keep
NaN. Snowflake fixed-point non-key columns represent ordinary NaN values as SQL
``NULL``, retaining the row and column. Singer logs a warning for each affected
column; FastSync applies the conversion during export. PostgreSQL numeric
primary keys use canonical text in both Snowflake table formats, regardless of
their declared precision and scale. This preserves ``NaN`` and distinct finite
keys without relying on floating-point identity. MariaDB/MySQL decimal keys
retain supported numeric mappings.
PostgreSQL logical replication requests numeric string output from wal2json 2.6
or later to preserve nonfinite values. Older plugins fall back with a warning
and retain their existing conversion of these values to NULL. LOG_BASED tables
with numeric primary keys need wal2json 2.6 or later if those keys can contain
``NaN`` or infinity; an older plugin cannot recover an identity it emits as NULL.

PostgreSQL retains supported declarations, including unconstrained ``NUMERIC``.
Targets older than PostgreSQL 15 use unconstrained ``NUMERIC`` for negative scale
or scale greater than precision. This preserves values without requiring syntax
that the older server does not support.

The mapping is computed before loading from the source declaration and target
capabilities. Source dimensions remain unchanged in the catalog even when the
destination uses FLOAT. The migration setting is tap configuration propagated to
the Snowflake loader; it adds no history to ``properties.json`` or ``state.json``.
This upgrade is intended for roll-forward deployment.

When a source non-key fixed-point decimal type, precision, or scale changes,
Singer targets and Snowflake PartialSync rename the old column and add the newly
mapped column.
Names use a UTC timestamp including microseconds, for example
``AMOUNT_20260929_120000_123456``. Historical rows have ``NULL`` in the new column
until those rows are replicated again. No resync or backfill is required.
An unchanged effective mapping does not create another version. Native Snowflake
readers can briefly see the original column name absent between rename and add.
If interrupted there, an unchanged retry adds the replacement column and resumes.
FastSync only authorizes this versioning for source decimal columns. A genuine
source floating-point or integer column with a mismatched target numeric type
remains incompatible and needs FullSync.

Existing Snowflake ``FLOAT``, ``DOUBLE``, or ``REAL`` columns created by the
legacy decimal mapping remain in place by default, so replication continues with
its existing floating-point precision. This includes primary keys whose new
precision-preserving mapping is text. Their load projection continues to use the
existing floating-point type. Set
``force_precision_columns: true`` at the MariaDB/MySQL or
PostgreSQL tap root to archive each eligible non-key legacy column and add its
precision-preserving replacement. This opt-in applies to native and managed Iceberg tables
and to Singer and supported PartialSync routes. It does not affect new columns.
Legacy floating-point decimal primary keys remain floating-point even when the
option is enabled, so one such table cannot stop the other streams in its tap.
Run ``import_config`` after changing the setting. A retained Iceberg PartialSync
attempt records the setting and its source-decimal columns in its internal
recovery manifest so a retry cannot silently change the planned schema action.

Other primary-key type changes are rejected before target schema changes.
PartialSync also rejects versioning its range column, because empty
values in the new column cannot identify historical rows in that range. Use
FullSync to replace the table in either case. The range restriction also applies
when Singer already versioned that column. A decimal range column that requires
FLOAT or text fallback also needs FullSync, because its target ordering cannot
represent the source range exactly. Newly created PostgreSQL numeric primary
keys on Snowflake, and other decimal primary keys that would require FLOAT,
use canonical, lossless text instead, preserving distinct row identities.
Changing an existing key's type still requires FullSync; leaving the option at
its default does not require one.

Legacy floating-point decimal bookmarks trigger a warning and conservative
source-side replay from below the rounded boundary. Nonfinite boundaries request
a full stream replay. Successful replication writes exact string bookmarks into
the existing state field. No additional migration marker is stored. This avoids
introducing skipped rows; it cannot restore data already missed by older runs.

Iceberg PartialSync retains recognized historical column versions. FullSync
keeps its existing full-table replacement behavior and recreates the current
source schema, removing historical versions. Existing recovery manifests retain
the planned archive names and expected historical column types so retries between
rename and add do not rename twice and publication detects unexpected schema drift.
