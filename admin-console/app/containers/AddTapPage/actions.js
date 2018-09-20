import {
  ADD_TAP,
  ADD_TAP_SUCCESS,
  ADD_TAP_ERROR,

  SET_ADD_TAP_BUTTON_STATE,
 } from './constants';

export function addTap(targetId, newTap) {
  return {
    type: ADD_TAP,
    targetId,
    newTap,
  }
}

export function tapAdded(response) {
  return {
    type: ADD_TAP_SUCCESS,
    response,
  }
}

export function tapAddedError(error) {
  return {
    type: ADD_TAP_ERROR,
    error,
  }
}

export function setAddTapButtonState(enabled) {
  return {
    type: SET_ADD_TAP_BUTTON_STATE,
    enabled,
  }
}
