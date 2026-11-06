# Native-to-Iceberg conversion lifecycle

Read [the core guidance](../../AGENTS.md) for the safety contract and the
[operator guide](../../../docs/connectors/targets/snowflake_iceberg.rst) for
configuration, feasibility reports and manual recovery. Keep this procedure
aligned with that guide and the implementation.

## Entry and prerequisites

Tap-root `native_to_iceberg_conversion` is core-only, never a target setting.
It runs one `run_tap` step inside the pidfile, before FastSync/Singer. The child
installs handlers that only exit; `stop_tap` stops the whole tap run.

Refuse before any Snowflake work unless generated settings carry Iceberg v3 and
flattening level 0. Refuse conversion while the catalog is older than
`inheritable_config.json`.

## Plan every stream

Use target-snowflake naming; block any FastSync/Singer name disagreement.
Classify with one `SHOW TABLES` per schema and lock-free manifest peeks.
De-duplicate physical work only: every stream's blockers and TEXT/VARIANT
classes count. A blocked stream holds back its table unless the primary is
absent. A class conflict also blocks a new or already-Iceberg table.

Block native primaries with unmanifested `_ICEBERG` companions. Reserved
companion names must not collide with another planned/existing table or another
tap's destination. Never call `convert()` for an already-Iceberg table without
a manifest.

## Resume before starting new conversions

Resume every pending `eventual=iceberg` attempt first, interrupted cutovers
first, continuing past a failure so other missing names can recover. Planning,
reserved-name, shared-writer, class or stale-catalog blockers hold back native
and promoted primaries. Pass them as `refusals` for an interrupted cutover: the
converter restores the name, then refuses before another cutover.

## Preflight, convert and release writers

Preflight every native candidate before starting a new conversion. Merge all
streams' TEXT/VARIANT classes for each table; resumed attempts repeat these
checks before cutover. Convert smallest first. Require every table to be new
or Iceberg before writers start; any failure stops the tap. Keep conversions
completed earlier in the run.

## Imported writers and manual conversion

Block a table another imported tap writes unless that tap is Iceberg v3 with
conversion enabled under the same target. That tap then converts or waits on
the shared lock before writing. Taps under other targets with the same account
and dbname always block. Merge the eligible taps' TEXT/VARIANT classes.

Manual conversion applies the classes of every imported tap writing the table
and passes conflicts as refusals. Skip top-level objects that a deeper-flattening
imported tap splits into child columns.

## Reports and FastSync

`--preflight-only` takes no lock, manifest or DDL. `fast_sync` never converts;
it refuses native targets before resetting a source slot, but may rebuild a
missing primary beside leftover companions.
