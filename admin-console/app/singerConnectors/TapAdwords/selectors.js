/**
 * TapZendesk selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapZendesk = state => state.get('tapZendesk', initialState);

const makeSelectStreams = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('streams'));

const makeSelectConfig = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('forceReloadConfig'))

const makeSelectForceRefreshStreams = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('forceRefreshStreams'))

const makeSelectActiveStream = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('activeStream'));

const makeSelectActiveStreamId = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('activeStreamId'));

const makeSelectLoading = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('saveConfigButtonEnabled'));

const makeSelectTestingConnection = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('testingConnection'));

const makeSelectTestingConnectionError = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('testingConnectionError'));

const makeSelectTestingConnectionSuccess = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('testingConnectionSuccess'));

const makeSelectTestConnectionButtonEnabled = () =>
    createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('testConnectionButtonEnabled'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapZendesk, tapZendeskState => tapZendeskState.get('consoleOutput'));

export {
  selectTapZendesk,
  
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
