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
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,
  streams: false,

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
    case LOAD_STREAMS_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.streams.status !== 200 ? action.streams.message : false)
        .set('streams', action.streams.result)
    case LOAD_STREAMS_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('streams', false)


    case SET_ACTIVE_STREAM_ID:
      return state.set('activeStreamId', action.streamId);

    case UPDATE_STREAM_TO_REPLICATE:
      return state
        .set('loading', true)
        .set('error', false)
    case UPDATE_STREAM_TO_REPLICATE_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('forceRefreshStreams', action.response.status === 200)
    case UPDATE_STREAM_TO_REPLICATE_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)

    default:
      return state;
  }
}

export default tapPostgresReducer;
