.. _transformations:

Load-time transformations
=========================

PipelineWise can transform selected fields during ingestion. PostgreSQL and
MySQL/MariaDB FastSync to Snowflake apply top-level transformations in the source
``SELECT``, before CSV generation, for both FullSync and PartialSync and both
native and managed Iceberg v3 targets. Local export files, S3 staging, archived
load files, and Snowflake staging therefore receive only the configured output.
There is no subsequent Snowflake transformation ``UPDATE`` on these routes.

Conditional rules intentionally retain the value when their conditions do not
match; columns without transformations are unchanged. Configure rules accordingly
before treating them as a security boundary. Other FastSync routes retain their
existing target-side transformation behaviour. Singer applies transformations
between the tap and target. Perform joins, aggregations, and analytical modelling
after ingestion.

Source-side masking uses source database CPU instead of Snowflake update work.
Measure that load when scheduling bulk exports; a 64-character hash can increase
the size of a short input even though raw values no longer need to be transferred.


Transformation types
--------------------

.. list-table:: Available
   :header-rows: 1
   :widths: 34 28 38
   :width: 100%

   * - Type
     - Compatible input
     - Output
   * - ``SET-NULL``
     - Any supported field
     - ``NULL``.
   * - ``HASH``
     - String
     - SHA-256 representation.
   * - ``HASH-SKIP-FIRST-n``
     - String; ``n`` is 1–9
     - First ``n`` characters plus a hash of the remainder.
   * - ``MASK-DATE``
     - Date or timestamp
     - Date masked to 1 January.
   * - ``MASK-NUMBER``
     - Numeric
     - Zero.
   * - ``MASK-HIDDEN``
     - String
     - ``hidden``.
   * - ``MASK-STRING-SKIP-ENDS-n``
     - String; ``n`` is 1–9
     - Preserves the first and last ``n`` characters and masks the middle.

For ``MASK-STRING-SKIP-ENDS-n``, values no longer than ``2 * n`` are fully
masked.


Mapped column types
-------------------

Transformations do not choose or change the target column type. FastSync uses
its existing source-to-Snowflake mappings; Singer uses its existing schema
mappings. Managed Iceberg uses the selected version's canonical physical types.
FastSync validates each rule against that mapped type before export:

.. list-table:: Snowflake FastSync type support
   :header-rows: 1
   :widths: 38 62

   * - Mapped type
     - Supported transformations
   * - ``VARCHAR``
     - ``SET-NULL``, ``HASH``, ``HASH-SKIP-FIRST-n``, ``MASK-HIDDEN``,
       ``MASK-STRING-SKIP-ENDS-n``
   * - ``NUMBER``, ``FLOAT`` / Iceberg ``DOUBLE``
     - ``SET-NULL``, ``MASK-NUMBER``
   * - ``DATE``, ``TIMESTAMP_NTZ``
     - ``SET-NULL``, ``MASK-DATE``
   * - ``BOOLEAN``, ``TIME``, ``BINARY``, ``VARIANT``
     - ``SET-NULL``

For example, MySQL ``VARBINARY`` maps to ``BINARY``, so hashing it is rejected;
it is not silently remapped to text. Fixed-size string outputs must fit the
mapped width. Unknown mapped types and unsupported combinations fail before
CSV creation. Correct the configuration rather than relying on an implicit cast.

Existing native PartialSync columns must also accept the mapped values without
changing type family or losing width or precision. Compatible narrow text
columns still widen automatically. Iceberg retains its stricter canonical
type and recovery checks. See :ref:`partial_sync_cases`.


Configuration
-------------

.. code-block:: yaml

   tables:
     - table_name: "users"
       replication_method: "LOG_BASED"
       transformations:
         - column: "email"
           type: "HASH"
         - column: "phone"
           type: "MASK-STRING-SKIP-ENDS-2"


Nested JSON fields
------------------

``field_paths`` applies a transformation to selected paths inside the JSON
object named by ``column``. A condition can similarly inspect
``when[].field_path``.

.. warning::

   PipelineWise rejects ``field_paths`` and ``when[].field_path`` for every
   connector pair that has a FastSync component: MariaDB/MySQL, PostgreSQL, or
   MongoDB sources targeting PostgreSQL or Snowflake. This route-level check
   applies even when the configured table would use Singer replication. On
   those routes, transform only top-level columns or omit the nested condition.


.. _conditional_transformations:

Conditions
----------

``when`` applies a transformation only when every condition matches:

.. code-block:: yaml

   transformations:
     - column: "value"
       type: "SET-NULL"
       when:
         - column: "field_name"
           regex_match: "password|secret|token"
         - column: "environment"
           equals: "production"

Conditions can inspect a top-level column. Nested ``field_path`` conditions are
subject to the FastSync-route restriction above. Multiple entries use logical
AND.


Snowflake FastSync semantics
----------------------------

Source-side execution preserves the existing Snowflake FastSync order:

1. Conditional transformations run individually in configuration order. Each
   condition sees the result of preceding conditional transformations.
2. Unconditional transformations then run together against that resulting row,
   regardless of their position among the conditional rules in the configuration.

This differs from Singer's sequential configuration order. Avoid depending on
the difference in tables that switch between FastSync and Singer.

Other existing Singer differences also remain: ``HASH-SKIP-FIRST-n`` slices
the hash input by UTF-8 bytes rather than characters; regex conditions search
within the string; and equality conditions with ``0``, ``false``, or ``null``
are ignored and do not trigger a rule on their own. For PostgreSQL JSON
object/array schemas, Singer can load a
transformed ``None`` as JSON ``null`` rather than FastSync's SQL ``NULL``.
These differences are tested explicitly; matching column types alone does not
guarantee identical transformation results.

String operations count characters, not UTF-8 bytes. Hashes remain hexadecimal
SHA-256. ``NULL`` and empty strings stay distinct; ``MASK-HIDDEN`` and
``MASK-NUMBER`` replace even ``NULL`` with their configured constant. Date masking
retains the time and fractional seconds. Existing export normalization, including
MySQL/MariaDB NUL removal and invalid-date handling, happens before transformation.

Equality comparisons are case-sensitive and preserve trailing spaces. Regex
conditions accept text and integer columns; equality requires matching string,
numeric, or Boolean values, or ``NULL``. Implicit date/string/numeric coercions
and floating-point regex rendering are rejected.

Regex conditions match the whole string, are case-sensitive, and ``.`` does not match
a newline. Source execution accepts a portable regex subset: literals, character
ranges, groups, alternation, ordinary repetitions, and bounded repetitions up to
255. Engine-specific constructs such as lookarounds, backreferences, inline flags,
locale character classes, and character-class set operators such as ``[a&&b]``
are rejected rather than delegated to a different regex engine with potentially
different results.
Backslashes in equality values or regex patterns are also rejected: the previous
Snowflake SQL literals interpreted them as escapes, so treating them literally
could change which rows are masked. Use an equivalent unambiguous condition
(for example, ``[.]`` for a literal dot) and verify its intended matches.

Unknown or ambiguous columns, nested paths, incompatible types, unsupported
condition coercions, and duplicate unconditional assignments to one column fail
before an export file is opened. There is no fallback to exporting raw values or
running the transformation after loading. Correct the rule or source schema and
retry; do not disable the transformation to bypass this guard.
Managed-Iceberg runs validate the projection before creating a new recovery
attempt, so a rejected rule does not leave a manifest blocking its correction.
Existing recovery attempts still require their original transformation configuration.

PartialSync still selects its inclusive range against original source values,
before transformations. Bookmarks and target range predicates remain unchanged.
Use an untransformed boundary column so the source and target ranges remain
comparable; hashing or masking that column does not translate the target range.

FastSync rejects transformations on an ``INCREMENTAL`` replication-key column
before querying its maximum: resuming requires the original value in local state
and recovery manifests, which would bypass the transformation boundary. Choose
a non-sensitive, untransformed replication key or another replication method.
Range arguments and dynamic-boundary results remain operational metadata and
can appear in logs/recovery manifests; do not use sensitive values as boundaries.


.. _transformation_validation:

Validation and rollout
----------------------

``pipelinewise validate`` checks source-side rule syntax, portable regexes,
duplicate unconditional rules, and transformed INCREMENTAL keys without
connecting to databases. ``import_config`` repeats those checks before discovery
and validates fields against the Singer catalog. FastSync checks mapped column
types and source capabilities at runtime; passing offline validation does not
prove that an existing target is compatible. See :ref:`partial_sync_cases`.

MySQL/MariaDB regex conditions require ICU/PCRE support. FastSync checks the
export connection with a literal-only query before using regex conditions;
unsupported engines fail before CSV creation. The result is reused only for
that connection. Equality-only or unconditional rules do not run this probe.

Transformation changes affect newly extracted records only. To update historical
target values, perform a controlled resync and verify that temporary files,
staging tables, logs, and rejected records do not expose the original value.

Test deterministic output, ``NULL`` handling, nested paths, conditional misses,
and schema changes before relying on a transformation as a security boundary.

Before upgrading to source-side execution, complete pending managed-Iceberg
recovery for transformed streams with the previous version. Retained attempts
from target-side execution are rejected because their staging may contain raw
values. They block replication; they do not automatically restart from scratch.
Do not delete recovery manifests to bypass this check. Include pending recovery
in the deployment handover to the on-call operator.

The upgrade does not rewrite or remove historical staging/archive files. Treat
pre-upgrade FastSync exports as potentially containing unmasked source values.
Review access, retention, retained object versions, and rollback needs separately;
do not assume a successful resync removes old files.

On the first representative bulk exports, compare source CPU, replica lag, and
export duration with the previous baseline. Include wide columns and tables with
many conditional rules; small development benchmarks do not establish production
capacity.

Optional monitoring can count unexpected output shapes without logging values.
For a column whose final, unconditional transformation is ``HASH``:

.. code-block:: sql

   SELECT COALESCE(COUNT_IF(
              "EMAIL" IS NOT NULL
              AND NOT REGEXP_LIKE("EMAIL", '[0-9a-f]{64}', 'c')
          ), 0) AS unexpected_hash_values
     FROM replicated_schema.customers;

This permits SQL NULL and checks lowercase hexadecimal SHA-256 shape only; it
does not prove the correct input was hashed. Do not apply it unchanged to
conditional hashes, prefix-preserving hashes, or columns with later rules.
Keep deterministic transformation tests as the correctness check; do not restore
the old UPDATE, which would transform values twice.
