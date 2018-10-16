/*
 * TapKafkaConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapKafka/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapKafka/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapKafka/LOAD_STREAMS_ERROR';

export const LOAD_CONFIG = 'TapKafka/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TapKafka/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TapKafka/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TapKafka/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapKafka/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapKafka/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TapKafka/SET_SAVE_CONFIG_BUTTON_STATE';

export const TEST_CONNECTION = 'TapKafka/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapKafka/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapKafka/TEST_CONNECTION_ERROR';
export const SET_TEST_CONNECTION_BUTTON_STATE = 'TapKafka/SET_TEST_CONNECTION_BUTTON_STATE';

export const SET_ACTIVE_STREAM_ID = 'TapKafka/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAMS = 'TapKafka/UPDATE_STREAMS';
export const UPDATE_STREAMS_SUCCESS = 'TapKafka/UPDATE_STREAMS_SUCCESS';
export const UPDATE_STREAMS_ERROR = 'TapKafka/UPDATE_STREAMS_ERROR';

export const UPDATE_STREAM = 'TapKafka/UPDATE_STREAM';
export const UPDATE_STREAM_SUCCESS = 'TapKafka/UPDATE_STREAM_SUCCESS';
export const UPDATE_STREAM_ERROR = 'TapKafka/UPDATE_STREAM_ERROR';

export const SET_TRANSFORMATION = 'TapKafka/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapKafka/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapKafka/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapKafka/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapKafka/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapKafka/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapKafka/RESET_CONSOLE_OUTPUT';
