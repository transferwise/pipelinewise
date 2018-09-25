import { fromJS } from 'immutable';

import {
  LOAD_RUN_LOGS,
  LOAD_RUN_LOGS_SUCCESS,
  LOAD_RUN_LOGS_ERROR,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,
  logs: false,
});

function tapRunLogsReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_RUN_LOGS:
      return state
        .set('loading', true)
        .set('error', false)
    case LOAD_RUN_LOGS_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.logs.status !== 200 ? action.logs.message : false)
        .set('logs', action.logs.result)
    case LOAD_RUN_LOGS_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('logs', false)

    default:
      return state;
  }
}

export default tapRunLogsReducer;
