/*
 * TapPostgresReducer
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

  SET_ACTIVE_STREAM_ID,

  UPDATE_STREAM_TO_REPLICATE,
  UPDATE_STREAM_TO_REPLICATE_SUCCESS,
  UPDATE_STREAM_TO_REPLICATE_ERROR,

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

  streams: false,

  activeStream: false,
  activeStreamId: false,

  forceRefreshStreams: false,
});

function tapPostgresReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_STREAMS:
      return state
        .set('loading', true)
        .set('error', false)
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


    case SET_ACTIVE_STREAM_ID:
      return state
        .set('activeStream', action.stream)
        .set('activeStreamId', action.streamId);

    case UPDATE_STREAM_TO_REPLICATE:
      return state
        .set('loading', true)
        .set('error', false)
        .set('consoleOutput', false)
    case UPDATE_STREAM_TO_REPLICATE_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('forceRefreshStreams', action.response.status === 200)
        .set('consoleOutput', false)
    case UPDATE_STREAM_TO_REPLICATE_ERROR:
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
        const result = action.response.result;
        let consoleOutput = false;
        if (result.returncode && result.returncode !== 0) {
          consoleOutput = `${result.stdout} - ${result.stderr}`;
        }

        return state
          .set('loading', false)
          .set('error', consoleOutput ? "Failed to run discovery mode" : false)
          .set('consoleOutput', consoleOutput)
          .set('forceRefreshStreams', consoleOutput ? false : true)
    case DISCOVER_TAP_ERROR:
        return state
          .set('loading', false)
          .set('error', action.error)
          .set('consoleOutput', false)
    
    case RESET_CONSOLE_OUTPUT:
        return state
          .set('error', false)
          .set('consoleOutput', false)

    default:
      return state;
  }
}

export default tapPostgresReducer;
