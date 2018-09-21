import {
  DELETE_TAP,
  DELETE_TAP_SUCCESS,
  DELETE_TAP_ERROR,

  SET_DELETE_TAP_BUTTON_STATE,
 } from './constants';

export function deleteTap(targetId, tapId, deleteTap) {
  return {
    type: DELETE_TAP,
    targetId,
    tapId,
    deleteTap,
  }
}

export function tapDeleted(response) {
  return {
    type: DELETE_TAP_SUCCESS,
    response,
  }
}

export function tapDeletedError(error) {
  return {
    type: DELETE_TAP_ERROR,
    error,
  }
}

export function setDeleteTapButtonState(enabled) {
  return {
    type: SET_DELETE_TAP_BUTTON_STATE,
    enabled,
  }
}
