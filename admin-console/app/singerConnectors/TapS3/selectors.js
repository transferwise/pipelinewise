/**
 * TapS3 selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapS3 = state => state.get('tapS3', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('forceReloadConfig'))

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('forceRefreshStreams'))

const makeSelectActiveStream = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapS3, tapS3State => tapS3State.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapS3, tapS3State => tapS3State.get('consoleOutput'));

export {
  selectTapS3,

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
