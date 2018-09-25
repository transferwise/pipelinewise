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

export {
  selectTapRunLogs,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectLogs,
};
