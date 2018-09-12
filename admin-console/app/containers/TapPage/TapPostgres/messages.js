import { defineMessages } from 'react-intl';

export default defineMessages({
  tablesToReplicate: {
    id: 'PipelineWise.TapPostgres.tablesToReplicate',
    defaultMessage: 'Tables to Replicate',
  },
  columnsToReplicate: {
    id: 'PipelineWise.TapPostgres.columnsToReplicate',
    defaultMessage: 'Fields to Replicate',
  },
  database: {
    id: 'PipelineWise.TapPostgres.database',
    defaultMessage: 'Database',
  },
  discover: {
    id: 'PipelineWise.TapPostgres.discover',
    defaultMessage: 'Discover',
  },
  discoverErrorTitle: {
    id: 'PipelineWise.TapPostgres.discoverErrorTitle',
    defaultMessage: 'Failed to discover schema',
  },
  schema: {
    id: 'PipelineWise.TapPostgres.schema',
    defaultMessage: 'Schema',
  },
  table: {
    id: 'PipelineWise.TapPostgres.table',
    defaultMessage: 'Table',
  },
  isView: {
    id: 'PipelineWise.TapPostgres.isView',
    defaultMessage: 'View?',
  },
  rowCount: {
    id: 'PipelineWise.TapPostgres.rowCount',
    defaultMessage: 'Approx Rows',
  },
  replicationMethod: {
    id: 'PipelineWise.TapPostgres.replicationMethod',
    defaultMessage: 'Method',
  },
  transformation: {
    id: 'PipelineWise.TapPostgres.transformation',
    defaultMessage: 'Transformation',
  },
  replicationMethodFullTable: {
    id: 'PipelineWise.TapPostgfres.replicationMethodFullTable',
    defaultMessage: 'Full',
  },
  replicationMethodLogBased: {
    id: 'PipelineWise.TapPostgfres.replicationMethodLogBased',
    defaultMessage: 'Log Based Incremental',
  },
  replicationMethodKeyBased: {
    id: 'PipelineWise.TapPostgfres.replicationMethodKeyBased',
    defaultMessage: 'Key Based Incremental',
  },
  selected: {
    id: 'PipelineWise.TapPostgres.selected',
    defaultMessage: 'Tracked',
  },
  notSelected: {
    id: 'PipelineWise.TapPostgres.notSelected',
    defaultMessage: 'Not Tracked',
  },
  automatic: {
    id: 'PipelineWise.TapPostgres.automatic',
    defaultMessage: 'Automatic',
  },
  streamNotSelected: {
    id: 'PipelineWise.TapPostgres.streamNotSelected',
    defaultMessage: 'Select a table to see fields',
  },
  column: {
    id: 'PipelineWise.TapPostgres.column',
    defaultMessage: 'Field Name',
  },
  type: {
    id: 'PipelineWise.TapPostgres.type',
    defaultMessage: 'Type',
  },
  primaryKey: {
    id: 'PipelineWise.TapPostgres.primaryKey',
    defaultMessage: 'Key',
  },
  newTable: {
    id: 'PipelineWise.TapPostgres.newTable',
    defaultMessage: 'New Table',
  },
  modifiedTable: {
    id: 'PipelineWise.TapPostgres.modifiedTable',
    defaultMessage: 'Table Schema Changed',
  },
  newColumn: {
    id: 'PipelineWise.TapPostgres.newColumn',
    defaultMessage: 'New Column',
  },
  modifiedColumn: {
    id: 'PipelineWise.TapPostgres.modifiedColumn',
    defaultMessage: 'Data Type Changed',
  },
});