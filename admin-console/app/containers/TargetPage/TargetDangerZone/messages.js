import { defineMessages } from 'react-intl';

export default defineMessages({
  title: {
    id: 'PipelineWise.containers.TargetDangerZone.title',
    defaultMessage: 'Danger Zone'
  },
  targetId: {
    id: 'PipelineWise.containers.TargetDangerZone.targetId',
    defaultMessage: 'Data Warehouse ID'
  },
  enterTargetIdToDelete: {
    id: 'PipelineWise.containers.TargetDangerZone.enterTargetIdToDelete',
    defaultMessage: 'Enter Data Warehouse id to delete. This will delete every child data source too.',
  },
  delete: {
    id: 'PipelineWise.containers.TargetDangerZone.delete',
    defaultMessage: 'Delete Data Warehouse',
  },
  deleteTargetSuccess: {
    id: 'PipelineWise.containers.TargetDangerZone.deleteTargetSuccess',
    defaultMessage: 'Target deleted',
  },
  targetToDeleteNotCorrect: {
    id: 'PipelineWise.containers.TargetDangerZone.targetToDeleteNotCorrect',
    defaultMessage: 'Target id doesn\'t match. Not deleting.',
  }
})