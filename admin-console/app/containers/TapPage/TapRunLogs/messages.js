import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TapRunLogs.title',
    defaultMessage: 'Data Sync Logs'
  },
  noLog: {
    id: 'PipelineWise.containers.TapRunLogs.noLog',
    defaultMessage: 'No log files. You need to run data sync at least one to see log files.',
  },
  refresh: {
    id: 'PipelineWise.containers.TapRunLogs.refresh',
    defaultMessage: 'Refresh',
  },
  createdAt: {
    id: 'PipelineWise.containers.TapRunLogs.created',
    defaultMessage: 'Created At',
  },
  status: {
    id: 'PipelineWise.containers.TapRunLogs.status',
    defaultMessage: 'Status',
  },
  statusRunning: {
    id: 'PipelineWise.containers.TapRunLogs.statusRunning',
    defaultMessage: 'Running',
  },
  statusSuccess: {
    id: 'PipelineWise.containers.TapRunLogs.statusSuccess',
    defaultMessage: 'Success',
  },
  statusFailed: {
    id: 'PipelineWise.containers.TapRunLogs.statusFailed',
    defaultMessage: 'Failed',
  },
  statusUnknown: {
    id: 'PipelineWise.containers.TapRunLogs.statusUnknown',
    defaultMessage: 'Unknown',
  },
})