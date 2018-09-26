import { fromJS } from 'immutable';

import {
  LOAD_RUN_LOGS,
  LOAD_RUN_LOGS_SUCCESS,
  LOAD_RUN_LOGS_ERROR,

  LOAD_LOG_VIEWER,
  LOAD_LOG_VIEWER_SUCCESS,
  LOAD_LOG_VIEWER_ERROR,

  SET_LOG_VIEWER_VISIBLE,
  RESET_LOG_VIEWER,

  SET_ACTIVE_LOG_ID,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,
  logs: false,

  viewerLoading: false,
  viewerError: false,
  log: false,

  logViewerVisible: false,

  activeLogId: false,
});

function tapRunLogsReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_RUN_LOGS:
      return state
        .set('loading', true)
        .set('error', false)
        .set('logViewerVisible', false)
        .set('viewerLoading', false)
        .set('viewerError', false)
        .set('log', false)
    case LOAD_RUN_LOGS_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.logs.status !== 200 ? action.logs.message : false)
        .set('logs', action.logs.result)
        .set('logViewerVisible', false)
        .set('viewerLoading', false)
        .set('viewerError', false)
        .set('log', false)
    case LOAD_RUN_LOGS_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('logs', false)
        .set('logViewerVisible', false)
        .set('viewerLoading', false)
        .set('viewerError', false)
        .set('log', false)

    case LOAD_LOG_VIEWER:
      return state
        .set('logViewerVisible', true)
        .set('viewerLoading', true)
        .set('viewerError', false)
        .set('log', false)
    case LOAD_LOG_VIEWER_SUCCESS:
      return state
        .set('logViewerVisible', true)
        .set('viewerLoading', false)
        .set('viewerError', action.log.status !== 200 ? action.log.message : false)
        .set('log', action.log.result)
    case LOAD_LOG_VIEWER_ERROR:
      return state
        .set('logViewerVisible', true)
        .set('viewerLoading', false)
        .set('viewerError', action.error)
        .set('log', false)

    case RESET_LOG_VIEWER:
      return state
        .set('logViewerVisible', false)
        .set('viwerLoading', false)
        .set('viewerErrr', false)
        .set('log', false)

    case SET_LOG_VIEWER_VISIBLE:
      return state
        .set('logViewerVisible', action.visible)

    case SET_ACTIVE_LOG_ID:
      return state
        .set('activeLogId', action.logId)
        .set('viewerLoading', false)
        .set('viewerError', false)
        .set('log', false)

    default:
      return state;
  }
}

export default tapRunLogsReducer;
