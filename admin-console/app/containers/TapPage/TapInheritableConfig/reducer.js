import { fromJS } from 'immutable';

import {
  LOAD_INHERITABLE_CONFIG,
  LOAD_INHERITABLE_CONFIG_SUCCESS,
  LOAD_INHERITABLE_CONFIG_ERROR,

  SAVE_INHERITABLE_CONFIG,
  SAVE_INHERITABLE_CONFIG_SUCCESS,
  SAVE_INHERITABLE_CONFIG_ERROR,

  SET_SAVE_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: true,
  error: false,

  saving: false,
  savingError: false,
  savingSuccess: false,
  saveButtonEnabled: false,

  inheritableConfig: false,
});

function tapInheritableConfigReducer(state = initialState, action) {
  switch (action.type) {
    case LOAD_INHERITABLE_CONFIG:
      return state
        .set('loading', true)
        .set('error', false)
        .set('savingError', false)
        .set('savingSuccess', false)
    case LOAD_INHERITABLE_CONFIG_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.inheritableConfig.status !== 200 ? action.inheritableConfig.message : false)
        .set('inheritableConfig', action.inheritableConfig.result)
    case LOAD_INHERITABLE_CONFIG_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('inheritableConfig', false)

    case SAVE_INHERITABLE_CONFIG:
      return state
        .set('saving', true)
        .set('savingError', false)
        .set('savingSuccess', false)
    case SAVE_INHERITABLE_CONFIG_SUCCESS:
      return state
        .set('saving', false)
        .set('savingError', action.response.status !== 200 ? action.response.message : false)
        .set('savingSuccess', true)
        .set('saveButtonEnabled', false)
    case SAVE_INHERITABLE_CONFIG_ERROR:
      return state
        .set('saving', false)
        .set('savingError', action.error)
        .set('savingSuccess', false)

    case SET_SAVE_BUTTON_STATE:
      return state
        .set('saveButtonEnabled', action.enabled)

    default:
      return state;
  }
}

export default tapInheritableConfigReducer;
