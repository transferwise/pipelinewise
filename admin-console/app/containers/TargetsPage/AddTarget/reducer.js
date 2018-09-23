import { fromJS } from 'immutable';

import {
  ADD_TARGET,
  ADD_TARGET_SUCCESS,
  ADD_TARGET_ERROR,

  SET_SUCCESS,
  SET_ADD_TARGET_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: false,
  error: false,
  success: false,

  newTarget: false,
  addTargetButtonEnabled: false,
});

function addTargetReducer(state = initialState, action) {
  switch (action.type) {
    case ADD_TARGET:
      return state
        .set('loading', false)
        .set('error', false)
        .set('success', false)
    case ADD_TARGET_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('success', action.response.status === 200)
        .set('newTarget', action.response.result)
    case ADD_TARGET_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('newTarget', false)
        .set('success', false)
    
    case SET_SUCCESS:
      return state
        .set('success', action.success)

    case SET_ADD_TARGET_BUTTON_STATE:
      return state
        .set('addTargetButtonEnabled', action.enabled)

    default:
      return state;
  }
}

export default addTargetReducer;
