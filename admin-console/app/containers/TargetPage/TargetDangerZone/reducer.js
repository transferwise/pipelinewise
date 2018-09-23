import { fromJS } from 'immutable';

import {
  DELETE_TARGET,
  DELETE_TARGET_SUCCESS,
  DELETE_TARGET_ERROR,

  SET_DELETE_TARGET_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: false,
  error: false,
  success: false,

  targetToDelete: false,
  deleteTargetButtonEnabled: false,
});

function deleteTargetReducer(state = initialState, action) {
  switch (action.type) {
    case DELETE_TARGET:
      return state
        .set('loading', false)
        .set('error', false)
        .set('success', false)
    case DELETE_TARGET_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('success', action.response.status === 200)
    case DELETE_TARGET_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('targetToDelete', false)
        .set('success', false)

    case SET_DELETE_TARGET_BUTTON_STATE:
      return state
        .set('deleteTargetButtonEnabled', action.enabled)

    default:
      return state;
  }
}

export default deleteTargetReducer;
