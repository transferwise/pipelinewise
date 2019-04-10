/*
 * TapS3 Actions
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

  LOAD_CONFIG,
  LOAD_CONFIG_SUCCESS,
  LOAD_CONFIG_ERROR,

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

export function loadConfig(targetId, tapId) {
  return {
    type: LOAD_CONFIG,
    targetId,
    tapId,
  }
}

export function configLoaded(config) {
  return {
    type: LOAD_CONFIG_SUCCESS,
    config,
  }
}

export function loadConfigError(error) {
  return {
    type: LOAD_CONFIG_ERROR,
    error,
  }
}

export function saveConfig(targetId, tapId, config) {
  return {
    type: SAVE_CONFIG,
    targetId,
    tapId,
    config
  }
}

export function saveConfigDone(response) {
  return {
    type: SAVE_CONFIG_SUCCESS,
    response,
  }
}

export function saveConfigError(error) {
  return {
    type: SAVE_CONFIG_ERROR,
    error,
  }
}

export function setSaveConfigButtonState(enabled) {
  return {
    type: SET_SAVE_CONFIG_BUTTON_STATE,
    enabled,
  }
}

export function testConnection(targetId, tapId, config) {
  return {
    type: TEST_CONNECTION,
    targetId,
    tapId,
    config,
  }
}

export function testConnectionSucces(response) {
  return {
    type: TEST_CONNECTION_SUCCESS,
    response,
  }
}

export function testConnectionError(error) {
  return {
    type: TEST_CONNECTION_ERROR,
    error,
  }
}

export function setTestConnectionButtonState(enabled) {
  return {
    type: SET_TEST_CONNECTION_BUTTON_STATE,
    enabled,
  }
}

export function setActiveStreamId(stream, streamId) {
  return {
    type: SET_ACTIVE_STREAM_ID,
    stream,
    streamId,
  };
}

export function updateStreams(targetId, tapId, params) {
  return {
    type: UPDATE_STREAMS,
    targetId,
    tapId,
    params,
  };
}

export function updateStreamsDone(response) {
  return {
    type: UPDATE_STREAMS_SUCCESS,
    response,
  };
}

export function updateStreamsError(error) {
  return {
    type: UPDATE_STREAMS_ERROR,
    error,
  };
}

export function updateStream(targetId, tapId, streamId, params) {
  return {
    type: UPDATE_STREAM,
    targetId,
    tapId,
    streamId,
    params,
  };
}

export function updateStreamDone(response) {
  return {
    type: UPDATE_STREAM_SUCCESS,
    response,
  };
}

export function updateStreamError(error) {
  return {
    type: UPDATE_STREAM_ERROR,
    error,
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
