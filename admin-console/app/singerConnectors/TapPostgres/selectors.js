/**
 * TapPostgres selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapPostgres = state => state.get('tapPostgres', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('forceReloadConfig'))

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

const makeSelectSavingSuccess = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapPostgres, tapPostgresState => tapPostgresState.get('consoleOutput'));

export {
  selectTapPostgres,
  
  makeSelectStreams,
  makeSelectConfig,
  makeSelectForceReloadConfig,
  makeSelectForceRefreshStreams,
  makeSelectActiveStream,
  makeSelectActiveStreamId,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveConfigButtonEnabled,
  makeSelectTestingConnection,
  makeSelectTestingConnectionError,
  makeSelectTestingConnectionSuccess,
  makeSelectTestConnectionButtonEnabled,
  makeSelectConsoleOutput,
};
