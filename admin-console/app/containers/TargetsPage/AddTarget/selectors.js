/**
 * AddTarget selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectAddTarget = state => state.get('addTarget', initialState);

const makeSelectNewTarget = () =>
  createSelector(selectAddTarget, addTargetState => addTargetState.get('newTarget'));

const makeSelectLoading = () =>
  createSelector(selectAddTarget, addTargetState => addTargetState.get('loading'));

const makeSelectError = () =>
  createSelector(selectAddTarget, addTargetState => addTargetState.get('error'));

const makeSelectSuccess = () =>
  createSelector(selectAddTarget, addTargetState => addTargetState.get('success'));

const makeSelectAddTargetButtonEnabled = () =>
  createSelector(selectAddTarget, addTargetState => addTargetState.get('addTargetButtonEnabled'));

export {
  selectAddTarget,

  makeSelectNewTarget,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,

  makeSelectAddTargetButtonEnabled,
};
