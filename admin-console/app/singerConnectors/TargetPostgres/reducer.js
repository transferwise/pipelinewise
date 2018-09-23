/*
 * TargetPostgresReducer
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
  LOAD_CONFIG,
  LOAD_CONFIG_ERROR,
  LOAD_CONFIG_SUCCESS,

  SAVE_CONFIG,
  SAVE_CONFIG_SUCCESS,
  SAVE_CONFIG_ERROR,
  SET_SAVE_CONFIG_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,

  saving: false,
  savingError: false,
  savingSuccess: false,
  saveConfigButtonEnabled: false,

  config: false,

  forceReloadConfig: false,
});

function targetPostgresReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_CONFIG:
      return state
        .set('loading', true)
        .set('error', false)
        .set('savingError', false)
        .set('savingSuccess', false)
        .set('forceReloadConfig', false)
    case LOAD_CONFIG_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.config.status !== 200 ? action.config.message : false)
        .set('config', action.config.result)
    case LOAD_CONFIG_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('config', false)

    case SAVE_CONFIG:
      return state
        .set('saving', true)
        .set('savingError', false)
        .set('savingSuccess', false)
    case SAVE_CONFIG_SUCCESS:
      return state
        .set('saving', false)
        .set('savingError', action.response.status !== 200 ? action.response.message : false)
        .set('savingSuccess', true)
        .set('saveConfigButtonEnabled', false)
        .set('forceReloadConfig', action.response.status === 200)
    case SAVE_CONFIG_ERROR:
      return state
        .set('saving', false)
        .set('savingError', action.error)
        .set('savingSuccess', false)
    case SET_SAVE_CONFIG_BUTTON_STATE:
      return state
        .set('saveConfigButtonEnabled', action.enabled)

    default:
      return state;
  }
}

export default targetPostgresReducer;
