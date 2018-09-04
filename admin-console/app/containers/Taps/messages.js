import { defineMessages } from 'react-intl';

export default defineMessages({
  tapsTopic: {
    id: 'analyticsdb.containers.Taps.header',
    defaultMessage: 'Integrations',
  },
  name: {
    id: 'analyticsdb.container.Taps.name',
    defaultMessage: 'Name',
  },
  status: {
    id: 'analyticsdb.container.Taps.status',
    defaultMessage: 'Status',
  },
  lastSyncAt: {
    id: 'analyticsdb.container.Taps.lastSyncAt',
    defaultMessage: 'Last Sync Completed',
  },
  flowsDetailsTopic: {
    id: 'analyticsdb.containers.TapsPage.detailsHeader',
    defaultMessage: 'Properties',
  },
  refresh: {
    id: 'analyticsdb.containers.TapsPage.refresh',
    defaultMessage: 'Refresh',
  },
  error: {
    id: 'analyticsdb.components.TapsList.error.message',
    defaultMessage: 'Something went wrong, please try again!',
  },
  flowItemNotSelected: {
    id: 'analyticsdb.components.TapDetailsViewer.flowItemNotSelected',
    defaultMessage: 'Select a flow, tap or target to see details.',
  },
  codeNotFound: {
    id: 'analyticsdb.components.TapDetailsViewer.codeNotFound',
    defaultMessage: 'JSON not found',
  }
});
