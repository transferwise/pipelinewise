import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TapInheritabelConfig.title',
    defaultMessage: 'Settings passed on to any Destination',
  },
  schema: {
    id: 'PipelineWise.containers.TapInheritabelConfig.schema',
    defaultMessage: 'Target Schema',
  },
  schemaHelp: {
    id: 'PipelineWise.containers.TapInheritabelConfig.schemaHelp',
    defaultMessage: 'Schema name in Destination DWH where the data will be synced. Defaults to \`public\'',
  },
  grantSelectTo: {
    id: 'PipelineWise.containers.TapInheritabelConfig.grantSelectTo',
    defaultMessage: 'Grant SELECT to',
  },
  grantSelectToHelp: {
    id: 'PipelineWise.containers.TapInheritabelConfig.grantSelectToHelp',
    defaultMessage: 'SELECT privilege will be granted to this role if the schema not exist',
  },
  batchSize: {
    id: 'PipelineWise.containers.TapInheritabelConfig.batchSize',
    defaultMessage: 'Batch Size',
  },
  batchSizeHelp: {
    id: 'PipelineWise.containers.TapInheritabelConfig.batchSizeHelp',
    defaultMessage: 'Number of maximum rows to INSERT/ UPDATE in one transaction. Defaults to 10000',
  },
  save: {
    id: 'PipelineWise.containers.TapInheritabelConfig.save',
    defaultMessage: 'Save',
  },
});