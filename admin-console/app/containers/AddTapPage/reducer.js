import { fromJS } from 'immutable';

import {
  ADD_TAP,
  ADD_TAP_SUCCESS,
  ADD_TAP_ERROR,

  SET_ADD_TAP_BUTTON_STATE,
} from './constants';

// The initial state of the App
export const initialState = fromJS({
  loading: false,
  error: false,
  success: false,

  newTap: false,
  addTapButtonEnabled: false,

  forceRedirectToConnectionsPage: false,
});

function addTapReducer(state = initialState, action) {
  switch (action.type) {
    case ADD_TAP:
      return state
        .set('loading', false)
        .set('error', false)
        .set('success', false)
        .set('forceRedirectToConnectionsPage', false)
    case ADD_TAP_SUCCESS:
    console.log(action)
      return state
        .set('loading', false)
        .set('error', action.response.status !== 200 ? action.response.message : false)
        .set('success', action.response.status === 200)
    case ADD_TAP_ERROR:
      return state
        .set('loading', false)
        .set('error', action.error)
        .set('newTap', false)
        .set('success', false)
        .set('forceRedirectToConnectionsPage', false)

    case SET_ADD_TAP_BUTTON_STATE:
        return state
          .set('addTapButtonEnabled', action.enabled)
          .set('forceRedirectToConnectionsPage', false)

    default:
      return state;
  }
}

export default addTapReducer;
