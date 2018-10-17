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
  batchSize: {
    id: 'PipelineWise.containers.TapInheritabelConfig.batchSize',
    defaultMessage: 'Batch Size',
  },
  batchSizeHelp: {
    id: 'PipelineWise.containers.TapInheritabelConfig.batchSizeHelp',
    defaultMessage: 'Number of maximum rows to INSERT/ UPDATE in one transaction. Defaults to 10000',
  },
  grantSelectTo: {
    id: 'PipelineWise.containers.TapInheritabelConfig.grantSelectTo',
    defaultMessage: 'Grant SELECT',
  },
  grantSelectToHelp: {
    id: 'PipelineWise.containers.TapInheritabelConfig.grantSelectToHelp',
    defaultMessage: 'SELECT privilege will be granted to this role if the schema not exist',
  },
  role: {
    id: 'PipelineWise.containers.TapInheritabelConfig.role',
    defaultMessage: 'Role or User',
  },
  createIndices: {
    id: 'PipelineWise.containers.TapInheritableConfig.createIndices',
    defaultMessage: 'Create Indices',
  },
  createIndicesHelp: {
    id: 'PipelineWise.containers.TapInheritableConfig.createIndicesHelp',
    defaultMessage: 'Indices will be created automatically on columns',
  },
  table: {
    id: 'PipelineWise.containers.TapInheritableConfig.table',
    defaultMessage: 'Table',
  },
  columns: {
    id: 'PipelineWise.containers.TapInheritableConfig.columns',
    defaultMessage: 'Column',
  },
  save: {
    id: 'PipelineWise.containers.TapInheritabelConfig.save',
    defaultMessage: 'Save',
  },
});