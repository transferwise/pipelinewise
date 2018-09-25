import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TapControlCard.title',
    defaultMessage: 'Danger Zone'
  },
  tapId: {
    id: 'PipelineWise.containers.TapControlCard.tapId',
    defaultMessage: 'Tap ID'
  },
  tapType: {
    id: 'PipelineWise.containers.TapControlCard.tapType',
    defaultMessage: 'Tap Type'
  },
  tapName: {
    id: 'PipelineWise.containers.TapControlCard.tapName',
    defaultMessage: 'Tap Name'
  },
  runTap: {
    id: 'PipelineWise.containers.TapControlCard.runTap',
    defaultMessage: 'Sync Data Now',
  },
  runTapSuccess: {
    id: 'PipelineWise.containers.TapControlCard.runTapSuccess',
    defaultMessage: 'Sync started in the background. Check log for details.',
  },
  runTapFailed: {
    id: 'PipelineWise.containers.TapControlCard.runTapFailed',
    defaultMessage: 'Tap sync failed to start. Check log for details.',
  },
  runTapError: {
    id: 'PipelineWise.containers.TapControlCard.runTapFailed',
    defaultMessage: 'Cannot start sync.',
  },
})