/**
 * TapRunLogs selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapRunLogs = state => state.get('tapRunLogs', initialState);

const makeSelectLoading = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('error'));

const makeSelectLogs = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('logs'))

  const makeSelectViewerLoading = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('viewerLoading'));

const makeSelectViewerError = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('viewerError'));

const makeSelectLog = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('log'))

const makeSelectLogViewerVisible = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('logViewerVisible'))

const makeSelectActiveLogId = () =>
  createSelector(selectTapRunLogs, TapRunLogsState => TapRunLogsState.get('activeLogId'))

export {
  selectTapRunLogs,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectLogs,

  makeSelectViewerLoading,
  makeSelectViewerError,
  makeSelectLog,

  makeSelectLogViewerVisible,

  makeSelectActiveLogId,
};
