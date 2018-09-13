/*
 * TapPostgresConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapPostgres/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapPostgres/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapPostgres/LOAD_STREAMS_ERROR';

export const SAVE_CONFIG = 'TapPostgres/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapPostgres/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapPostgres/SAVE_CONFIG_ERROR';

export const TEST_CONNECTION = 'TapPostgres/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapPostgres/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapPostgres/TEST_CONNECTION_ERROR';

export const SET_ACTIVE_STREAM_ID = 'TapPostgres/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAM_TO_REPLICATE = 'TapPostgres/UPDATE_STREAM_TO_REPLICATE';
export const UPDATE_STREAM_TO_REPLICATE_SUCCESS = 'TapPostgres/UPDATE_STREAM_TO_REPLICATE_SUCCESS';
export const UPDATE_STREAM_TO_REPLICATE_ERROR = 'TapPostgres/UPDATE_STREAM_TO_REPLICATE_ERROR';

export const SET_TRANSFORMATION = 'TapPostgres/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapPostgres/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapPostgres/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapPostgres/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapPostgres/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapPostgres/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapPostgres/RESET_CONSOLE_OUTPUT';
