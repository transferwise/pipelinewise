/*
 * TapAdwordskConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapAdwords/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapAdwords/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapAdwords/LOAD_STREAMS_ERROR';

export const LOAD_CONFIG = 'TapAdwords/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TapAdwords/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TapAdwords/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TapAdwords/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapAdwords/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapAdwords/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TapAdwords/SET_SAVE_CONFIG_BUTTON_STATE';

export const TEST_CONNECTION = 'TapAdwords/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapAdwords/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapAdwords/TEST_CONNECTION_ERROR';
export const SET_TEST_CONNECTION_BUTTON_STATE = 'TapAdwords/SET_TEST_CONNECTION_BUTTON_STATE';

export const SET_ACTIVE_STREAM_ID = 'TapAdwords/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAMS = 'TapAdwords/UPDATE_STREAMS';
export const UPDATE_STREAMS_SUCCESS = 'TapAdwords/UPDATE_STREAMS_SUCCESS';
export const UPDATE_STREAMS_ERROR = 'TapAdwords/UPDATE_STREAMS_ERROR';

export const UPDATE_STREAM = 'TapAdwords/UPDATE_STREAM';
export const UPDATE_STREAM_SUCCESS = 'TapAdwords/UPDATE_STREAM_SUCCESS';
export const UPDATE_STREAM_ERROR = 'TapAdwords/UPDATE_STREAM_ERROR';

export const SET_TRANSFORMATION = 'TapAdwords/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapAdwords/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapAdwords/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapAdwords/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapAdwords/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapAdwords/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapAdwords/RESET_CONSOLE_OUTPUT';
