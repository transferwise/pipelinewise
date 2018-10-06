import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TapControlCard.title',
    defaultMessage: 'Data Source Summary'
  },
  tap: {
    id: 'PipelineWise.containers.TapControlCard.target',
    defaultMessage: 'Data Source'
  },
  target: {
    id: 'PipelineWise.containers.TapControlCard.target',
    defaultMessage: 'Destination'
  },
  tapId: {
    id: 'PipelineWise.containers.TapControlCard.tapId',
    defaultMessage: 'Data Source ID'
  },
  tapType: {
    id: 'PipelineWise.containers.TapControlCard.tapType',
    defaultMessage: 'Tap Type'
  },
  tapName: {
    id: 'PipelineWise.containers.TapControlCard.tapName',
    defaultMessage: 'Tap Name'
  },
  tapOwner: {
    id: 'PipelineWise.containers.TapControlCard.tapOwner',
    defaultMessage: 'Data Source Owner'
  },
  status: {
    id: 'PipelineWise.containers.TapControlCard.status',
    defaultMessage: 'Status',
  },
  lastTimestamp: {
    id: 'PipelineWise.container.TapControlCard.lastTimestamp',
    defaultMessage: 'Last Sync (UTC)',
  },
  lastStatus: {
    id: 'PipelineWise.container.TapControlCard.lastStatus',
    defaultMessage: 'Last Sync Result',
  },
  syncPeriod: {
    id: 'PipelineWise.container.TapControlCard.syncPeriod',
    defaultMessage: 'Sync Period',
  },
  runTap: {
    id: 'PipelineWise.containers.TapControlCard.runTap',
    defaultMessage: 'Sync Data Now',
  },
  runTapSuccess: {
    id: 'PipelineWise.containers.TapControlCard.runTapSuccess',
    defaultMessage: 'Data Sync started. Check logs for details.',
  },
  runTapFailed: {
    id: 'PipelineWise.containers.TapControlCard.runTapFailed',
    defaultMessage: 'Data Sync failed to start. Check logs for details.',
  },
  runTapError: {
    id: 'PipelineWise.containers.TapControlCard.runTapFailed',
    defaultMessage: 'Cannot start sync.',
  },
})
