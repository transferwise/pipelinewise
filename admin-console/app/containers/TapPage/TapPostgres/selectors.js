/**
 * TapPostgres selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapPostgres = state => state.get('tapPostgres', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('streams'));

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('forceRefreshStreams'))

const makeSelectActiveStreamId = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('error'));

export {
  selectTapPostgres,
  
  makeSelectStreams,
  makeSelectForceRefreshStreams,
  makeSelectActiveStreamId,
  makeSelectLoading,
  makeSelectError,
};
