/**
 * The global state selectors
 */

import { createSelector } from 'reselect';

const selectGlobal = state => state.get('global');

const selectRoute = state => state.get('route');

const makeSelectTargetsLoading = () =>
  createSelector(selectGlobal, globalState => globalState.get('targetsLoading'));

const makeSelectTargetsError = () =>
  createSelector(selectGlobal, globalState => globalState.get('targetsError'));

const makeSelectTargets = () =>
  createSelector(selectGlobal, globalState => globalState.get('targets'));

const makeSelectTargetLoading = () =>
  createSelector(selectGlobal, globalState => globalState.get('targetLoading'));

const makeSelectTargetError = () =>
  createSelector(selectGlobal, globalState => globalState.get('targetError'));

const makeSelectTarget = () =>
  createSelector(selectGlobal, globalState => globalState.get('target'));

const makeSelectTapsLoading = () =>
  createSelector(selectGlobal, globalState => globalState.get('tapsLoading'));

const makeSelectTapsError = () =>
  createSelector(selectGlobal, globalState => globalState.get('tapsError'));

const makeSelectTaps = () =>
  createSelector(selectGlobal, globalState => globalState.get('taps'));

const makeSelectTapLoading = () =>
  createSelector(selectGlobal, globalState => globalState.get('tapLoading'));

const makeSelectTapError = () =>
  createSelector(selectGlobal, globalState => globalState.get('tapError'));

const makeSelectTap = () =>
  createSelector(selectGlobal, globalState => globalState.get('tap'));

const makeSelectLocation = () =>
  createSelector(selectRoute, routeState => routeState.get('location').toJS());

export {
  selectGlobal,

  makeSelectTargetsLoading,
  makeSelectTargetsError,
  makeSelectTargets,

  makeSelectTargetLoading,
  makeSelectTargetError,
  makeSelectTarget,

  makeSelectTapsLoading,
  makeSelectTapsError,
  makeSelectTaps,

  makeSelectTapLoading,
  makeSelectTapError,
  makeSelectTap,

  makeSelectLocation,
};
