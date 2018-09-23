import { defineMessages } from 'react-intl';

export default defineMessages({
  header: {
    id: 'PipelineWise.containers.Targets.header',
    defaultMessage: 'Data Warehouses',
  },
  addTarget: {
    id: 'PipelineWise.containers.Targets.addTarget',
    defaultMessage: 'Add Data Warehouse',
  },
  noTarget: {
    id: 'PipelineWise.containers.Targets.noTarget',
    defaultMessage: 'Target Data Warehouse is not defined. You need to create at least one Data Warehouse to load data from source systems.',
  },
  name: {
    id: 'PipelineWise.container.Targets.name',
    defaultMessage: 'Name',
  },
  status: {
    id: 'PipelineWise.container.Targets.status',
    defaultMessage: 'Status',
  },
  lastSyncAt: {
    id: 'PipelineWise.container.Targets.lastSyncAt',
    defaultMessage: 'Last Sync Completed',
  },
  statusNotConfigured: {
    id: 'PipelineWise.components.Targets.statusNotConfigured',
    defaultMessage: 'Not Configured',
  },
  statusReady: {
    id: 'PipelineWise.components.Targets.statusReady',
    defaultMessage: 'Ready to Run',
  },
  statusUnknown: {
    id: 'PipelineWise.components.Targets.statusUnknown',
    defaultMessage: 'Unkown',
  },
});