/**
 * TargetDangerZone selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTargetDangerZone = state => state.get('targetDangerZone', initialState);

const makeSelectTargetToDelete = () =>
  createSelector(selectTargetDangerZone, addTargetDangerZoneState => addTargetDangerZoneState.get('targetToDelete'));

const makeSelectLoading = () =>
  createSelector(selectTargetDangerZone, addTargetDangerZoneState => addTargetDangerZoneState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTargetDangerZone, addTargetDangerZoneState => addTargetDangerZoneState.get('error'));

const makeSelectSuccess = () =>
  createSelector(selectTargetDangerZone, addTargetDangerZoneState => addTargetDangerZoneState.get('success'));

const makeSelectDeleteTargetButtonEnabled = () =>
  createSelector(selectTargetDangerZone, addTargetDangerZoneState => addTargetDangerZoneState.get('deleteTargetButtonEnabled'));

export {
  selectTargetDangerZone,

  makeSelectTargetToDelete,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,

  makeSelectDeleteTargetButtonEnabled,
};
