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

const makeSelectActiveStream = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('error'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('consoleOutput'));

export {
  selectTapPostgres,
  
  makeSelectStreams,
  makeSelectForceRefreshStreams,
  makeSelectActiveStream,
  makeSelectActiveStreamId,
  makeSelectLoading,
  makeSelectError,
  makeSelectConsoleOutput,
};
