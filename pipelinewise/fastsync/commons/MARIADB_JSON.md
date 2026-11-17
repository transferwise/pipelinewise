# MariaDB JSON mapping

## Contract

After imported config enables JSON discovery, new Snowflake native and managed
Iceberg v3 MariaDB JSON columns use VARIANT.
Singer and PartialSync retain established TEXT columns, including their raw JSON
serialization. After import, FullSync creates replacement columns with the
VARIANT mapping. Native-to-Iceberg conversion preserves established TEXT or VARIANT;
it does not parse or retype the column. Ordinary LONGTEXT, Oracle MySQL JSON,
other sources and other destinations keep their existing behavior.

## Discovery and transport

`tap_mysql.discover_utils` recognizes the exact generated JSON_VALID constraint,
preserving spaces and escaped backticks inside identifiers. Core config derives
the internal `json_target` from the Snowflake destination. Existing v3 settings
remain accepted for older generated configs. Refresh native catalogs with
`import_config` to enable the mapping. Until the generated tap config contains
`json_target: snowflake`, native FastSync and Singer both keep the older TEXT
mapping, including during a resync.

Successful discovery records `mariadb-json-discovery` version 1 and the resolved
engine in every stream's root metadata, including streams without JSON columns.
A setting or one recognized JSON property does not prove the entire catalog was
refreshed. Native MySQL/MariaDB preflight requires valid evidence for every
selected stream before replacing the uncertainty blocker with setup advice.

The `mariadb-json` schema format distinguishes this contract from ordinary JSON.
Transport validates JSON but keeps the original serialized text. Retained TEXT
must not pass through JSON decoding/re-encoding or PARSE_JSON. New VARIANT loads
parse once, including keyless COPY. Preserve SQL NULL versus serialized JSON
`null`, all JSON root types, whitespace, Unicode, escapes and numeric spelling.

## Existing tables and recovery

`managed_iceberg.plan_column_changes` owns Singer retention. FastSync carries
`mariadb_json_columns` separately from decimal provenance. Native and Iceberg
PartialSync use the established TEXT type for staging, publication and source
transformation validation. Retained TEXT hash/mask rules keep text semantics;
new VARIANT columns retain the existing supported-transformation restrictions.
The legacy FLOAT flag does not control JSON retention. Existing v3 width checks
still apply.

`transform-field` validates MariaDB JSON string rules provisionally during
import, without querying Snowflake. On runtime SCHEMA messages it marks fields
requiring string semantics as `format: mariadb-json-text`. This includes whole
value hash/mask rules and JSON fields used by regex or non-null equality
conditions. The source catalog and state files retain their original formats;
the marker exists only in the Singer stream. Keep the input schema for condition
evaluation and do not decode or reserialize values.

`target-snowflake` accepts that marker only for an existing TEXT column. Refuse
missing or non-TEXT columns before DDL or upload, and retain the existing type
for keyed MERGE and keyless COPY. The marker cannot create a new TEXT column.
FullSync still applies the VARIANT transformation restrictions before export.
Unexpected transport types or transformation failures must stop the compatibility
path without emitting the original private value or including it in errors.
Preserve SQL NULL and conditional non-match behavior. Reject nested JSON paths
and numeric/date masks; top-level SET-NULL and SQL-null conditions do not require
TEXT.

New FullSync and PartialSync manifests persist JSON provenance. PartialSync also
persists its effective staging types.
Durable staged recovery uses its persisted type contract without reconnecting
to the source. Re-export validates current provenance against the saved attempt;
ambiguous affected attempts cannot silently change mapping. Keep unaffected
recovery identities compatible.

Conversion intersects every writer's acceptable TEXT/VARIANT classes. MariaDB
JSON alone permits either class; generated transformations that need string
semantics narrow that writer to TEXT before preflight or automatic conversion.
This includes JSON columns used only by regex or non-null equality conditions.
A strict writer also narrows the choice. Conflicting strict writers still block.
Copy and content validation retain the physical column class and values,
including on resumed attempts.

## Acceptance evidence

Cover native/v3, new/retained columns, keyed MERGE/keyless COPY, all JSON roots,
explicit exclusions, ordinary LONGTEXT and other destinations. Verify retained
text byte equality, no column versioning, effective transformation types, and
target type drift during recovery. Test a legacy string catalog refreshed by
import_config, followed by Singer and PartialSync hash/mask writes. Validate
the runtime TEXT requirement before mutation, including missing/VARIANT columns,
conditional and sparse records, SQL NULL, and sanitized transformation errors.
Converter tests must preserve physical TEXT
and VARIANT with no PARSE_JSON, retain generic mismatch refusals, reconcile all
writers, and reject missing/partial/malformed discovery evidence.

Operator rules are in
[schema changes](../../../docs/user_guide/schema_changes.rst) and
[Iceberg conversion](../../../docs/connectors/targets/snowflake_iceberg.rst).
