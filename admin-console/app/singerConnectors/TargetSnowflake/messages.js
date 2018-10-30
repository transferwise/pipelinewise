import { defineMessages } from 'react-intl';

export default defineMessages({
  save: {
    id: 'PipelineWise.TargetSnowflake.save',
    defaultMessage: 'Save',
  },
  saveConnectionError: {
    id: 'PipelineWise.TargetSnowflake.saveConnectionSuccess',
    defaultMessage: 'Cannot save connection details!',
  },
  saveConnectionSuccess: {
    id: 'PipelineWise.TargetSnowflake.saveConnectionSuccess',
    defaultMessage: 'Connection Saved.',
  },
  testConnection: {
    id: 'PipelineWise.TargetSnowflake.testConnection',
    defaultMessage: 'Test Connection',
  },
  testConnectionError: {
    id: 'PipelineWise.TargetSnowflake.testConnectionError',
    defaultMessage: 'Cannot connect to database!',
  },
  testConnectionSuccess: {
    id: 'PipelineWise.TargetSnowflake.testConnectionSuccess',
    defaultMessage: 'Test Connection Passed.',
  },
});