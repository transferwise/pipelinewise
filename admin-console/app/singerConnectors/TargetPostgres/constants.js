/*
 * TargetPostgresConstants
 * Each action has a corresponding type, which the reducer knows and picks up on.
 * To avoid weird typos between the reducer and the actions, we save them as
 * constants here. We prefix them with 'yourproject/YourComponent' so we avoid
 * reducers accidentally picking up actions they shouldn't.
 *
 * Follow this format:
 * export const YOUR_ACTION_CONSTANT = 'yourproject/YourContainer/YOUR_ACTION_CONSTANT';
 */

export const LOAD_CONFIG = 'TargetPostgres/LOAD_CONFIG';
export const LOAD_CONFIG_SUCCESS = 'TargetPostgres/LOAD_CONFIG_SUCCESS';
export const LOAD_CONFIG_ERROR = 'TargetPostgres/LOAD_CONFIG_ERROR';

export const SAVE_CONFIG = 'TargetPostgres/SAVE_CONFIG';
export const SAVE_CONFIG_SUCCESS = 'TargetPostgres/SAVE_CONFIG_SUCCESS';
export const SAVE_CONFIG_ERROR = 'TargetPostgres/SAVE_CONFIG_ERROR';
export const SET_SAVE_CONFIG_BUTTON_STATE = 'TargetPostgres/SET_SAVE_CONFIG_BUTTON_STATE';
