/*
 * TapS3Constants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapS3/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapS3/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapS3/LOAD_STREAMS_ERROR';

export const LOAD_CONFIG = 'TapS3/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TapS3/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TapS3/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TapS3/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapS3/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapS3/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TapS3/SET_SAVE_CONFIG_BUTTON_STATE';

export const TEST_CONNECTION = 'TapS3/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapS3/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapS3/TEST_CONNECTION_ERROR';
export const SET_TEST_CONNECTION_BUTTON_STATE = 'TapS3/SET_TEST_CONNECTION_BUTTON_STATE';

export const SET_ACTIVE_STREAM_ID = 'TapS3/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAMS = 'TapS3/UPDATE_STREAMS';
export const UPDATE_STREAMS_SUCCESS = 'TapS3/UPDATE_STREAMS_SUCCESS';
export const UPDATE_STREAMS_ERROR = 'TapS3/UPDATE_STREAMS_ERROR';

export const UPDATE_STREAM = 'TapS3/UPDATE_STREAM';
export const UPDATE_STREAM_SUCCESS = 'TapS3/UPDATE_STREAM_SUCCESS';
export const UPDATE_STREAM_ERROR = 'TapS3/UPDATE_STREAM_ERROR';

export const SET_TRANSFORMATION = 'TapS3/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapS3/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapS3/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapS3/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapS3/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapS3/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapS3/RESET_CONSOLE_OUTPUT';
