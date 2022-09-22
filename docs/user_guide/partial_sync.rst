.. _partial_sync_cases:

Different cases of partial resync
=================================

1. **Normal**

.. image:: ../img/partial_sync_case_1.png

This is the normal case which all source columns exist in the target.
after exporting data from the source into S3, a temp table will be created on Snowflake and then it will be merged
with the target table.

2. **Some columns are deleted from the target**

.. image:: ../img/partial_sync_case_2.png

``Col2`` is deleted from the target. after merging the temp and target tables only rows 2 and 3 in the target will have
value.

3. **Some columns are deleted from the source**

.. image:: ../img/partial_sync_case_3.png

In this example, ``Col2`` is removed from the source table and it causes the values for this column not be updated in
the synced table and row ``3`` has ``null`` value.

4. **Hard delete is disabled (soft delete)**

.. image:: ../img/partial_sync_case_4.png

If ``hard_delete`` setting  is ``false`` the records which are deleted from the source, won't be deleted from the target. they
just will have a time stamp in meta column.

5. **Hard delete is enabled**

.. image:: ../img/partial_sync_case_5.png

If ``hard_delete`` setting  is ``true`` the records which are deleted from the source, will be deleted from the target.

6. **Combination of all cases**

.. image:: ../img/partial_sync_case_all.png

