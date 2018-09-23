import {
  ADD_TARGET,
  ADD_TARGET_SUCCESS,
  ADD_TARGET_ERROR,

  SET_ADD_TARGET_BUTTON_STATE,
 } from './constants';

export function addTarget(newTarget) {
  return {
    type: ADD_TARGET,
    newTarget,
  }
}

export function targetAdded(response) {
  return {
    type: ADD_TARGET_SUCCESS,
    response,
  }
}

export function targetAddedError(error) {
  return {
    type: ADD_TARGET_ERROR,
    error,
  }
}

export function setAddTargetButtonState(enabled) {
  return {
    type: SET_ADD_TARGET_BUTTON_STATE,
    enabled,
  }
}
