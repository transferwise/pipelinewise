/*
 * TapMysqlConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapMysql/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapMysql/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapMysql/LOAD_STREAMS_ERROR';

export const LOAD_CONFIG = 'TapMysql/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TapMysql/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TapMysql/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TapMysql/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapMysql/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapMysql/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TapMysql/SET_SAVE_CONFIG_BUTTON_STATE';

export const TEST_CONNECTION = 'TapMysql/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapMysql/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapMysql/TEST_CONNECTION_ERROR';
export const SET_TEST_CONNECTION_BUTTON_STATE = 'TapMysql/SET_TEST_CONNECTION_BUTTON_STATE';

export const SET_ACTIVE_STREAM_ID = 'TapMysql/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAM = 'TapMysql/UPDATE_STREAM';
export const UPDATE_STREAM_SUCCESS = 'TapMysql/UPDATE_STREAM_SUCCESS';
export const UPDATE_STREAM_ERROR = 'TapMysql/UPDATE_STREAM_ERROR';

export const SET_TRANSFORMATION = 'TapMysql/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapMysql/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapMysql/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapMysql/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapMysql/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapMysql/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapMysql/RESET_CONSOLE_OUTPUT';
