import { defineMessages } from 'react-intl';

export default defineMessages({
  tapsTopic: {
    id: 'PipelineWise.containers.Taps.header',
    defaultMessage: 'Data Sources',
  },
  name: {
    id: 'PipelineWise.container.Taps.name',
    defaultMessage: 'Name',
  },
  status: {
    id: 'PipelineWise.container.Taps.status',
    defaultMessage: 'Status',
  },
  lastSyncAt: {
    id: 'PipelineWise.container.Taps.lastSyncAt',
    defaultMessage: 'Last Sync Completed',
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
  }
});
