import { defineMessages } from 'react-intl';

export default defineMessages({
  save: {
    id: 'PipelineWise.TargetPostgres.save',
    defaultMessage: 'Save',
  },
  saveConnectionError: {
    id: 'PipelineWise.TargetPostgres.saveConnectionSuccess',
    defaultMessage: 'Cannot save connection details!',
  },
  saveConnectionSuccess: {
    id: 'PipelineWise.TargetPostgres.saveConnectionSuccess',
    defaultMessage: 'Connection Saved.',
  },
  testConnection: {
    id: 'PipelineWise.TapTargetPostgresPostgres.testConnection',
    defaultMessage: 'Test Connection',
  },
  testConnectionError: {
    id: 'PipelineWise.TargetPostgres.testConnectionError',
    defaultMessage: 'Cannot connect to database!',
  },
  testConnectionSuccess: {
    id: 'PipelineWise.TargetPostgres.testConnectionSuccess',
    defaultMessage: 'Test Connection Passed.',
  },
});