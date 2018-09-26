import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TapDangerZone.title',
    defaultMessage: 'Danger Zone'
  },
  tapId: {
    id: 'PipelineWise.containers.TapDangerZone.tapId',
    defaultMessage: 'Data Source ID'
  },
  enterTapIdToDelete: {
    id: 'PipelineWise.containers.TapDangerZone.enterTapIdToDelete',
    defaultMessage: 'Enter data source id to delete',
  },
  delete: {
    id: 'PipelineWise.containers.TapDangerZone.delete',
    defaultMessage: 'Delete Data Source',
  },
  deleteTapSuccess: {
    id: 'PipelineWise.containers.TapDangerZone.deleteTapSuccess',
    defaultMessage: 'Tap deleted',
  },
  tapToDeleteNotCorrect: {
    id: 'PipelineWise.containers.TapDangerZone.tapToDeleteNotCorrect',
    defaultMessage: 'Tap id doesn\'t match. Not deleting.',
  }
})