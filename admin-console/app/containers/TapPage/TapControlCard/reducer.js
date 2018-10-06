import { fromJS } from 'immutable';

import {
  LOAD_TAP,
  LOAD_TAP_SUCCESS,
  LOAD_TAP_ERROR,

  SET_TAP_SYNC_PERIOD,
  SET_TAP_SYNC_PERIOD_SUCCESS,
  SET_TAP_SYNC_PERIOD_ERROR,

  RUN_TAP,
  RUN_TAP_SUCCESS,
  RUN_TAP_ERROR,

  RESET_CONSOLE_OUTPUT,

  SET_RUN_TAP_BUTTON_STATE,
} from './constants';
import messages from './messages';

// The initial state of the App
export const initialState = fromJS({
  tapLoading: true,
  tapError: false,
  tap: false,

  setTapSyncPeriodLoading: false,
  setTapSyncPeriodError: false,
  setTapSyncPeriodSuccess: false,

  runTapLoading: false,
  runTapError: false,
  runTapSuccess: false,
  
  consoleOutput: false,

  runTapButtonEnabled: false,

  forceRefreshTapControlCard: false,
});

function tapControlCardReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_TAP:
      return state
        .set('tapLoading', true)
        .set('tapError', false)
        .set('tap', false)
        .set('consoleOutput', false)
        .set('runTapSuccess', false)
        .set('forceRefreshTapControlCard', false)
    case LOAD_TAP_SUCCESS:
      return state
        .set('tapLoading', false)
        .set('tapError', action.tap.status !== 200 ? action.tap.message : false)
        .set('tap', action.tap.result)
        .set('consoleOutput', false)
        .set('runTapSuccess', false)
        .set('forceRefreshTapControlCard', false)
    case LOAD_TAP_ERROR:
      return state
        .set('tapLoading', false)
        .set('tapError', action.error)
        .set('tap', false)
        .set('consoleOutput', false)
        .set('runTapSuccess', false)
        .set('forceRefreshTapControlCard', false)
        .set('setTapSyncPeriodSuccess', false)

    case SET_TAP_SYNC_PERIOD:
      return state
        .set('setTapSyncPeriodLoading', true)
        .set('setTapSyncPeriodError', false)
        .set('setTapSyncPeriodSuccess', false)
        .set('forceRefreshTapControlCard', false)
    case SET_TAP_SYNC_PERIOD_SUCCESS:
      return state
        .set('setTapSyncPeriodLoading', false)
        .set('setTapSyncPeriodError', action.response.status !== 200 ? action.response.message : false)
        .set('setTapSyncPeriodSuccess', true)
        .set('forceRefreshTapControlCard', true)
    case SET_TAP_SYNC_PERIOD_ERROR:
      return state
        .set('setTapSyncPeriodLoading', false)
        .set('setTapSyncPeriodError', action.error)
        .set('setTapSyncPeriodSuccess', false)
        .set('forceRefreshTapControlCard', false)

    case RUN_TAP:
      return state
        .set('runTapLoading', true)
        .set('runTapError', false)
        .set('runTapSuccess', false)
        .set('consoleOutput', false)
        .set('runTapButtonEnabled', false)
        .set('forceRefreshTapControlCard', false)
        .set('setTapSyncPeriodSuccess', false)
    case RUN_TAP_SUCCESS:
      const rc = action.response && action.response.result && action.response.result.returncode
      const stdout = action.response && action.response.result && action.response.result.stdout
      const stderr = action.response && action.response.result && action.response.result.stderr
      const message = action.response && action.response.message
      
      return state
        .set('runTapLoading', false)
        .set('runTapError', rc !== 0 ? messages.runTapFailed.defaultMessage : false)
        .set('runTapSuccess', action.response.status === 200)
        .set('consoleOutput', rc !== 0 ? `${stdout} - ${stderr} (${message})` : false)
        .set('runTapButtonEnabled', true)
        .set('forceRefreshTapControlCard', false)
        .set('setTapSyncPeriodSuccess', false)
    case RUN_TAP_ERROR:
      return state
        .set('runTapLoading', false)
        .set('runTapError', action.error)
        .set('runTapSuccess', false)
        .set('consoleOutput', false)
        .set('runTapButtonEnabled', true)
        .set('forceRefreshTapControlCard', false)
        .set('setTapSyncPeriodSuccess', false)
    
    case RESET_CONSOLE_OUTPUT:
      return state
        .set('runTapLoading', false)
        .set('runTapError', false)
        .set('runTapSuccess', false)
        .set('consoleOutput', false)
        .set('forceRefreshTapControlCard', false)
        .set('setTapSyncPeriodSuccess', false)

    case SET_RUN_TAP_BUTTON_STATE:
        return state
          .set('runTapButtonEnabled', action.enabled)
          .set('consoleOutput', false)
          .set('forceRefreshTapControlCard', false)
          .set('setTapSyncPeriodSuccess', false)

    default:
      return state;
  }
}

export default tapControlCardReducer;
