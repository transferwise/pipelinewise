/**
 * TapAdwords selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapAdwords = state => state.get('tapAdwords', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('forceReloadConfig'))

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('forceRefreshStreams'))

const makeSelectActiveStream = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapAdwords, tapAdwordsState => tapAdwordsState.get('consoleOutput'));

export {
  selectTapAdwords,

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
