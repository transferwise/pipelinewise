import { defineMessages } from 'react-intl';

export default defineMessages({
  tablesToReplicate: {
    id: 'ETLWise.TapPostgres.tablesToReplicate',
    defaultMessage: 'Tables to Replicate',
  },
  columnsToReplicate: {
    id: 'ETLWise.TapPostgres.columnsToReplicate',
    defaultMessage: 'Fields to Replicate',
  },
  database: {
    id: 'ETLWise.TapPostgres.database',
    defaultMessage: 'Database',
  },
  schema: {
    id: 'ETLWise.TapPostgres.schema',
    defaultMessage: 'Schema',
  },
  table: {
    id: 'ETLWise.TapPostgres.table',
    defaultMessage: 'Table',
  },
  isView: {
    id: 'ETLWise.TapPostgres.isView',
    defaultMessage: 'View?',
  },
  rowCount: {
    id: 'ETLWise.TapPostgres.rowCount',
    defaultMessage: 'Approx Rows',
  },
  replicationMethod: {
    id: 'ETLWise.TapPostgres.replicationMethod',
    defaultMessage: 'Method',
  },
  replicationMethodFullTable: {
    id: 'ETLWise.TapPostgfres.replicationMethodFullTable',
    defaultMessage: 'Full',
  },
  replicationMethodLogBased: {
    id: 'ETLWise.TapPostgfres.replicationMethodLogBased',
    defaultMessage: 'Log Based Incremental',
  },
  replicationMethodKeyBased: {
    id: 'ETLWise.TapPostgfres.replicationMethodKeyBased',
    defaultMessage: 'Key Based Incremental',
  },
  selected: {
    id: 'ETLWise.TapPostgres.selected',
    defaultMessage: 'Tracked',
  },
  notSelected: {
    id: 'ETLWise.TapPostgres.notSelected',
    defaultMessage: 'Not Tracked',
  },
  automatic: {
    id: 'ETLWise.TapPostgres.automatic',
    defaultMessage: 'Automatic',
  },
  streamNotSelected: {
    id: 'ETLWise.TapPostgres.streamNotSelected',
    defaultMessage: 'Select a table to see fields',
  },
  column: {
    id: 'ETLWise.TapPostgres.column',
    defaultMessage: 'Field Name',
  },
  type: {
    id: 'ETLWise.TapPostgres.type',
    defaultMessage: 'Type',
  },
  primaryKey: {
    id: 'ETLWise.TapPostgres.primaryKey',
    defaultMessage: 'Key',
  },
});