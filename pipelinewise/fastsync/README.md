# FastSync

## Description

FastSync is a performance optimization that bypasses the Singer Specification for
bulk data operations. It is functionally identical to Full Table replication but
uses native database tools for optimised performance. Primary use case of FastSync
is initial sync or resyncing large tables with hundreds of millions of rows where
singer components would usually be running for long hours or sometimes for days.

PipelineWise detects automatically when FastSync gives better performance than the singer
components and uses it automatically whenever it’s possible.

## Supported tap-target routes


| Source        | Destination              |
|---------------|--------------------------|
| MySQL/MariaDB | * Snowflake<br />* Postgres |
| Postgres      | * Snowflake<br />* Postgres |
| MongoDB       | * Snowflake<br />* Postgres |
