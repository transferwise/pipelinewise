/**
 * TapControlCard selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapControlCard = state => state.get('tapControlCard', initialState);

const makeSelectTapLoading = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('tapLoading'));

const makeSelectTapError = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('tapError'));

const makeSelectTap = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('tap'));

const makeSelectSetTapSyncPeriodLoading = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('setTapSyncPeriodLoading'));

const makeSelectSetTapSyncPeriodError = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('setTapSyncPeriodError'));

const makeSelectSetTapSyncPeriodSuccess = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('setTapSyncPeriodSuccess'));

const makeSelectRunTapLoading = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapLoading'));

const makeSelectRunTapError = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapError'));

const makeSelectRunTapSuccess = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapSuccess'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('consoleOutput'));

const makeSelectRunTapButtonEnabled = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapButtonEnabled'));

const  makeSelectForceRefreshTapControlCard = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('forceRefreshTapControlCard'));

export {
  selectTapControlCard,

  makeSelectTapLoading,
  makeSelectTapError,
  makeSelectTap,

  makeSelectSetTapSyncPeriodLoading,
  makeSelectSetTapSyncPeriodError,
  makeSelectSetTapSyncPeriodSuccess,
  
  makeSelectRunTapLoading,
  makeSelectRunTapError,
  makeSelectRunTapSuccess,
  makeSelectConsoleOutput,

  makeSelectRunTapButtonEnabled,

  makeSelectForceRefreshTapControlCard
};
