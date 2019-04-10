/*
 * TapAdwordsReducer
 *
 * The reducer takes care of our data. Using actions, we can change our
 * application state.
 * To add a new action, add it to the switch statement in the reducer function
 *
 * Example:
 * case YOUR_ACTION_CONSTANT:
 *   return state.set('yourStateVariable', true);
 */
import { fromJS } from 'immutable';

import {
  LOAD_STREAMS,
  LOAD_STREAMS_SUCCESS,
  LOAD_STREAMS_ERROR,

  LOAD_CONFIG,
  LOAD_CONFIG_ERROR,
  LOAD_CONFIG_SUCCESS,

  SAVE_CONFIG,
  SAVE_CONFIG_SUCCESS,
  SAVE_CONFIG_ERROR,
  SET_SAVE_CONFIG_BUTTON_STATE,

  TEST_CONNECTION,
  TEST_CONNECTION_SUCCESS,
  TEST_CONNECTION_ERROR,
  SET_TEST_CONNECTION_BUTTON_STATE,

  SET_ACTIVE_STREAM_ID,

  UPDATE_STREAMS,
  UPDATE_STREAMS_SUCCESS,
  UPDATE_STREAMS_ERROR,

  UPDATE_STREAM,
  UPDATE_STREAM_SUCCESS,
  UPDATE_STREAM_ERROR,

  SET_TRANSFORMATION,
  SET_TRANSFORMATION_SUCCESS,
  SET_TRANSFORMATION_ERROR,

  DISCOVER_TAP,
  DISCOVER_TAP_SUCCESS,
  DISCOVER_TAP_ERROR,

  RESET_CONSOLE_OUTPUT,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,
  consoleOutput: false,

  saving: false,
  savingError: false,
  savingSuccess: false,
  saveConfigButtonEnabled: false,

  testingConnection: false,
  testingConnectionError: false,
  testingConnectionSuccess: false,
  testConnectionButtonEnabled: true,

  streams: false,
  config: false,

  activeStream: false,
  activeStreamId: false,

  forceReloadConfig: false,
  forceRefreshStreams: false,
});

function tapAdwordsReducer(state = initialState, action) {
  let result;
  let consoleOutput = false;

  switch (action.type) {
    case LOAD_STREAMS:
      return state
        .set('loading', true)
        .set('error', false)
        .set('forceReloadConfig', false)
        .set('forceRefreshStreams', false)
        .set('consoleOutput', false)
    case LOAD_STREAMS_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.streams.status !== 200 ? action.streams.message : false)
        .set('streams', action.streams.result)
        .set('consoleOutput', false)
    case LOAD_STREAMS_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('streams', false)
        .set('consoleOutput', false)

    case LOAD_CONFIG:
      return state
        .set('loading', true)
        .set('error', false)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('testingConnectionError', false)
        .set('testingConnectionSuccess', false)
        .set('forceReloadConfig', false)
        .set('forceRefreshStreams', false)
        .set('consoleOutput', false)
    case LOAD_CONFIG_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.config.status !== 200 ? action.config.message : false)
        .set('config', action.config.result)
        .set('consoleOutput', false)
    case LOAD_CONFIG_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('config', false)
        .set('consoleOutput', false)

    case SAVE_CONFIG:
      return state
        .set('saving', true)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('testingConnectionError', false)
        .set('testingConnectionSuccess', false)
        .set('consoleOutput', false)
    case SAVE_CONFIG_SUCCESS:
      return state
        .set('saving', false)
        .set('savingError', action.response.status !== 200 ? action.response.message : false)
        .set('savingSuccess', true)
        .set('testingConnectionError', false)
        .set('testingConnectionSucces', false)
        .set('testConnectionButtonEnabled', true)
        .set('saveConfigButtonEnabled', false)
        .set('forceReloadConfig', action.response.status === 200)
        .set('forceRefreshStreams', action.response.status === 200)
        .set('consoleOutput', false)
    case SAVE_CONFIG_ERROR:
      return state
        .set('saving', false)
        .set('savingError', action.error)
        .set('savingSuccess', false)
        .set('testingConnectionError', false)
        .set('testingConnectionSucces', false)
        .set('consoleOutput', false)
    case SET_SAVE_CONFIG_BUTTON_STATE:
      return state
        .set('saveConfigButtonEnabled', action.enabled)

    case TEST_CONNECTION:
      return state
        .set('testingConnection', true)
        .set('testingConnectionError', false)
        .set('testingConnectionSuccess', false)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('consoleOutput', false)
    case TEST_CONNECTION_SUCCESS:
      result = action.response.result;
      consoleOutput = false;
      if (result && result.returncode && result.returncode !== 0) {
        consoleOutput = `${result.stdout} - ${result.stderr}`;
      } else if(action.response && action.response.message) {
        consoleOutput = action.response.message;
      }

      return state
        .set('testingConnection', false)
        .set('testingConnectionError', consoleOutput ? "Failed to test connection" : false)
        .set('testingConnectionSuccess', true)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('consoleOutput', consoleOutput)
    case TEST_CONNECTION_ERROR:
      return state
        .set('testingConnection', false)
        .set('testingConnectionError', action.error)
        .set('testingConnectionSuccess', false)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('consoleOutput', false)
    case SET_TEST_CONNECTION_BUTTON_STATE:
      return state
        .set('testConnectionButtonEnabled', action.enabled)

    case SET_ACTIVE_STREAM_ID:
      return state
        .set('activeStream', action.stream)
        .set('activeStreamId', action.streamId);

    case UPDATE_STREAMS:
      return state
        .set('loading', true)
        .set('error', false)
        .set('consoleOutput', false)
    case UPDATE_STREAMS_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('forceReloadConfig', action.response.status === 200)
        .set('forceRefreshStreams', action.response.status === 200)
        .set('consoleOutput', false)
    case UPDATE_STREAMS_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('consoleOutput', false)

    case UPDATE_STREAM:
      return state
        .set('loading', true)
        .set('error', false)
        .set('consoleOutput', false)
    case UPDATE_STREAM_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('forceReloadConfig', action.response.status === 200)
        .set('forceRefreshStreams', action.response.status === 200)
        .set('consoleOutput', false)
    case UPDATE_STREAM_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('consoleOutput', false)

    case SET_TRANSFORMATION:
      return state
        .set('loading', true)
        .set('error', false)
        .set('consoleOutput', false)
    case SET_TRANSFORMATION_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('forceReloadConfig', action.response.status === 200)
        .set('forceRefreshStreams', action.response.status === 200)
        .set('consoleOutput', false)
    case SET_TRANSFORMATION_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('consoleOutput', false)

    case DISCOVER_TAP:
        return state
          .set('loading', true)
          .set('error', false)
          .set('consoleOutput', false)
    case DISCOVER_TAP_SUCCESS:
        result = action.response.result;
        consoleOutput = false;
        if (result && result.returncode && result.returncode !== 0) {
          consoleOutput = `${result.stdout} - ${result.stderr}`;
        } else if(action.response && action.response.message) {
          consoleOutput = action.response.message;
        }

        return state
          .set('loading', false)
          .set('error', consoleOutput ? "Failed to run discovery mode" : false)
          .set('consoleOutput', consoleOutput)
          .set('forceReloadConfig', consoleOutput ? false : true)
          .set('forceRefreshStreams', consoleOutput ? false : true)
    case DISCOVER_TAP_ERROR:
        return state
          .set('loading', false)
          .set('error', action.error)
          .set('consoleOutput', false)

    case RESET_CONSOLE_OUTPUT:
        return state
          .set('error', false)
          .set('savingError', false)
          .set('savingSuccess', false)
          .set('testingConnectionError', false)
          .set('testingConnectionSuccess', false)
          .set('consoleOutput', false)

    default:
      return state;
  }
}

export default tapAdwordsReducer;
