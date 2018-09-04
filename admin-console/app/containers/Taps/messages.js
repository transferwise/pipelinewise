import { defineMessages } from 'react-intl';

export default defineMessages({
  tapsTopic: {
    id: 'ETLWise.containers.Taps.header',
    defaultMessage: 'Integrations',
  },
  name: {
    id: 'ETLWise.container.Taps.name',
    defaultMessage: 'Name',
  },
  status: {
    id: 'ETLWise.container.Taps.status',
    defaultMessage: 'Status',
  },
  lastSyncAt: {
    id: 'ETLWise.container.Taps.lastSyncAt',
    defaultMessage: 'Last Sync Completed',
  },
  flowsDetailsTopic: {
    id: 'ETLWise.containers.TapsPage.detailsHeader',
    defaultMessage: 'Properties',
  },
  refresh: {
    id: 'ETLWise.containers.TapsPage.refresh',
    defaultMessage: 'Refresh',
  },
  error: {
    id: 'ETLWise.components.TapsList.error.message',
    defaultMessage: 'Something went wrong, please try again!',
  },
  flowItemNotSelected: {
    id: 'ETLWise.components.TapDetailsViewer.flowItemNotSelected',
    defaultMessage: 'Select a flow, tap or target to see details.',
  },
  codeNotFound: {
    id: 'ETLWise.components.TapDetailsViewer.codeNotFound',
    defaultMessage: 'JSON not found',
  }
});
