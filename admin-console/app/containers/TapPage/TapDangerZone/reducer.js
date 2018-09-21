import { fromJS } from 'immutable';

import {
  DELETE_TAP,
  DELETE_TAP_SUCCESS,
  DELETE_TAP_ERROR,

  SET_DELETE_TAP_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: false,
  error: false,
  success: false,

  tapToDelete: false,
  deleteTapButtonEnabled: false,
});

function deleteTapReducer(state = initialState, action) {
  switch (action.type) {
    case DELETE_TAP:
      return state
        .set('loading', false)
        .set('error', false)
        .set('success', false)
    case DELETE_TAP_SUCCESS:
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('success', action.response.status === 200)
    case DELETE_TAP_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('tapToDelete', false)
        .set('success', false)

    case SET_DELETE_TAP_BUTTON_STATE:
        return state
          .set('deleteTapButtonEnabled', action.enabled)

    default:
      return state;
  }
}

export default deleteTapReducer;
