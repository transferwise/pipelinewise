FastSync
--------

Fast Sync is one of the Replication Methods that is functionally identical to Full Table
replication but Fast Sync is bypassing the Singer Specification for optimised performance.
Primary use case of FastSync is initial sync or to resyncing large tables with hundreds of
millions of rows where singer component would usually be running for long hours or sometimes
for days.

PipelineWise detects automatically when Fast Sync gives better performance than the singer
components and uses it automatically whenever it’s possible.
