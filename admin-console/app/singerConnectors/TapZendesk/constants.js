/*
 * TapZendeskConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_STREAMS = 'TapZendesk/LOAD_STREAMS';
export const LOAD_STREAMS_SUCCESS = 'TapZendesk/LOAD_STREAMS_SUCCESS';
export const LOAD_STREAMS_ERROR = 'TapZendesk/LOAD_STREAMS_ERROR';

export const LOAD_CONFIG = 'TapZendesk/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TapZendesk/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TapZendesk/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TapZendesk/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TapZendesk/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TapZendesk/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TapZendesk/SET_SAVE_CONFIG_BUTTON_STATE';

export const TEST_CONNECTION = 'TapZendesk/TEST_CONNECTION';
export const TEST_CONNECTION_SUCCESS = 'TapZendesk/TEST_CONNECTION_SUCCESS';
export const TEST_CONNECTION_ERROR = 'TapZendesk/TEST_CONNECTION_ERROR';
export const SET_TEST_CONNECTION_BUTTON_STATE = 'TapZendesk/SET_TEST_CONNECTION_BUTTON_STATE';

export const SET_ACTIVE_STREAM_ID = 'TapZendesk/SET_ACTIVE_STREAM_ID';

export const UPDATE_STREAMS = 'TapZendesk/UPDATE_STREAMS';
export const UPDATE_STREAMS_SUCCESS = 'TapZendesk/UPDATE_STREAMS_SUCCESS';
export const UPDATE_STREAMS_ERROR = 'TapZendesk/UPDATE_STREAMS_ERROR';

export const UPDATE_STREAM = 'TapZendesk/UPDATE_STREAM';
export const UPDATE_STREAM_SUCCESS = 'TapZendesk/UPDATE_STREAM_SUCCESS';
export const UPDATE_STREAM_ERROR = 'TapZendesk/UPDATE_STREAM_ERROR';

export const SET_TRANSFORMATION = 'TapZendesk/SET_TRANSFORMATION';
export const SET_TRANSFORMATION_SUCCESS = 'TapZendesk/SET_TRANSFORMATION_SUCCESS';
export const SET_TRANSFORMATION_ERROR = 'TapZendesk/SET_TRANSFORMATION_ERROR';

export const DISCOVER_TAP = 'TapZendesk/DISCOVER_TAP';
export const DISCOVER_TAP_SUCCESS = 'TapZendesk/DISCOVER_TAP_SUCCESS';
export const DISCOVER_TAP_ERROR = 'TapZendesk/DISCOVER_TAP_ERROR';

export const RESET_CONSOLE_OUTPUT = 'TapZendesk/RESET_CONSOLE_OUTPUT';
