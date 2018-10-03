/**
 * TapMysql selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapMysql = state => state.get('tapMysql', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('forceReloadConfig'))

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('forceRefreshStreams'))

const makeSelectActiveStream = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapMysql, tapMysqlState => tapMysqlState.get('consoleOutput'));

export {
  selectTapMysql,
  
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
