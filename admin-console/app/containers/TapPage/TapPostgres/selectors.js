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

const makeSelectSaving = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('savingError'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testingConnectionError'));

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
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectTestingConnection,
  makeSelectTestingConnectionError,
  makeSelectConsoleOutput,
};
