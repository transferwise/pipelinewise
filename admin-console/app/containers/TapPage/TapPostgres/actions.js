/*
 * TapPostgres Actions
 *
 * Actions change things in your application
 * Since this boilerplate uses a uni-directional data flow, specifically redux,
 * we have these actions which are the only way your application interacts with
 * your application state. This guarantees that your state is up to date and nobody
 * messes it up weirdly somewhere.
 *
 * To add a new Action:
 * 1) Import your constant
 * 2) Add a function like this:
 *    export function yourAction(var) {
 *        return { type: YOUR_ACTION_CONSTANT, var: var }
 *    }
 */

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

export function loadStreams(targetId, tapId) {
  return {
    type: LOAD_STREAMS,
    targetId,
    tapId,
  }
}

export function streamsLoaded(streams) {
  return {
    type: LOAD_STREAMS_SUCCESS,
    streams,
  }
}

export function streamsLoadedError(error) {
  return {
    type: LOAD_STREAMS_ERROR,
    error,
  }
}

export function setActiveStreamId(stream, streamId) {
  return {
    type: SET_ACTIVE_STREAM_ID,
    stream,
    streamId,
  };
}

export function updateStreamToReplicate(targetId, tapId, streamId, params) {
  return {
    type: UPDATE_STREAM_TO_REPLICATE,
    targetId,
    tapId,
    streamId,
    params,
  };
}

export function setTransformation(targetId, tapId, stream, fieldId, value) {
  return {
    type: SET_TRANSFORMATION,
    targetId,
    tapId,
    stream,
    fieldId,
    value,
  }
}

export function setTransformationDone(response) {
  return {
    type: SET_TRANSFORMATION_SUCCESS,
    response,
  }
}

export function setTransformationError(error) {
  return {
    type: SET_TRANSFORMATION_ERROR,
    error,
  }
}

export function updateStreamToReplicateDone(response) {
  return {
    type: UPDATE_STREAM_TO_REPLICATE_SUCCESS,
    response,
  };
}

export function updateStreamToReplicateError(error) {
  return {
    type: UPDATE_STREAM_TO_REPLICATE_ERROR,
    error, 
  };
}

export function discoverTap(targetId, tapId) {
  return {
    type: DISCOVER_TAP,
    targetId,
    tapId,
  };
}

export function discoverTapDone(response) {
  return {
    type: DISCOVER_TAP_SUCCESS,
    response
  };
}

export function discoverTapError(error) {
  return {
    type: DISCOVER_TAP_ERROR,
    error,
  };
}

export function resetConsoleOutput() {
  return {
    type: RESET_CONSOLE_OUTPUT,
  }
}