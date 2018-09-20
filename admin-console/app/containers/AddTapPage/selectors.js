/**
 * AddTap selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectAddTap = state => state.get('addTap', initialState);

const makeSelectNewTap = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('newTap'));

const makeSelectLoading = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('loading'));

const makeSelectError = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('error'));

const makeSelectSuccess = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('success'));

const makeSelectAddTapButtonEnabled = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('addTapButtonEnabled'));

const makeSelectForceRedirectToConnectionsPage = () =>
  createSelector(selectAddTap, addTapState => addTapState.get('forceRedirectToConnectionsPage'));

export {
  selectAddTap,

  makeSelectNewTap,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,

  makeSelectAddTapButtonEnabled,

  makeSelectForceRedirectToConnectionsPage,
};
