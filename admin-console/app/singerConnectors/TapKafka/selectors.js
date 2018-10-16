/**
 * TapKafka selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapKafka = state => state.get('tapKafka', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('forceReloadConfig'))

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('forceRefreshStreams'))

const makeSelectActiveStream = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapKafka, tapKafkaState => tapKafkaState.get('consoleOutput'));

export {
  selectTapKafka,
  
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
