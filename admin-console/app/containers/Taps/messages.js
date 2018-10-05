import { defineMessages } from 'react-intl';

export default defineMessages({
  tapsTopic: {
    id: 'PipelineWise.containers.Taps.header',
    defaultMessage: 'Data Sources',
  },
  addSource: {
    id: 'PipelineWise.containers.Taps.addSource',
    defaultMessage: 'Add Data Source',
  },
  name: {
    id: 'PipelineWise.container.Taps.name',
    defaultMessage: 'Name',
  },
  owner: {
    id: 'PipelineWise.container.Taps.owner',
    defaultMessage: 'Owner',
  },
  syncPeriod: {
    id: 'PipelineWise.container.Taps.syncPeriod',
    defaultMessage: 'Sync Period',
  },
  status: {
    id: 'PipelineWise.container.Taps.status',
    defaultMessage: 'Status',
  },
  lastTimestamp: {
    id: 'PipelineWise.container.Taps.lastTimestamp',
    defaultMessage: 'Last Sync',
  },
  lastStatus: {
    id: 'PipelineWise.container.Taps.lastStatus',
    defaultMessage: 'Last Sync Result',
  },
  flowsDetailsTopic: {
    id: 'PipelineWise.containers.TapsPage.detailsHeader',
    defaultMessage: 'Properties',
  },
  refresh: {
    id: 'PipelineWise.containers.TapsPage.refresh',
    defaultMessage: 'Refresh',
  },
  error: {
    id: 'PipelineWise.components.TapsList.error.message',
    defaultMessage: 'Something went wrong, please try again!',
  },
  flowItemNotSelected: {
    id: 'PipelineWise.components.TapDetailsViewer.flowItemNotSelected',
    defaultMessage: 'Select a flow, tap or target to see details.',
  },
  codeNotFound: {
    id: 'PipelineWise.components.TapDetailsViewer.codeNotFound',
    defaultMessage: 'JSON not found',
  },
  statusNotConfigured: {
    id: 'PipelineWise.components.TapDetailsViewer.statusNotConfigured',
    defaultMessage: 'Not Configured',
  },
  statusReady: {
    id: 'PipelineWise.components.TapDetailsViewer.ready',
    defaultMessage: 'Ready to Run',
  },
  statusUnknown: {
    id: 'PipelineWise.components.TapDetailsViewer.statusUnknown',
    defaultMessage: 'Unkown',
  },
});
