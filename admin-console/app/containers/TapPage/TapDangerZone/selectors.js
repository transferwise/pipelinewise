/**
 * TapDangerZone selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapDangerZone = state => state.get('tapDangerZone', initialState);

const makeSelectTapToDelete = () =>
  createSelector(selectTapDangerZone, addTapDangerZoneState => addTapDangerZoneState.get('tapToDelete'));

const makeSelectLoading = () =>
  createSelector(selectTapDangerZone, addTapDangerZoneState => addTapDangerZoneState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapDangerZone, addTapDangerZoneState => addTapDangerZoneState.get('error'));

const makeSelectSuccess = () =>
  createSelector(selectTapDangerZone, addTapDangerZoneState => addTapDangerZoneState.get('success'));

const makeSelectDeleteTapButtonEnabled = () =>
  createSelector(selectTapDangerZone, addTapDangerZoneState => addTapDangerZoneState.get('deleteTapButtonEnabled'));

export {
  selectTapDangerZone,

  makeSelectTapToDelete,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,

  makeSelectDeleteTapButtonEnabled,
};
