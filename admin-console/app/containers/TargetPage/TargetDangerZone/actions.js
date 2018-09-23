import {
  DELETE_TARGET,
  DELETE_TARGET_SUCCESS,
  DELETE_TARGET_ERROR,

  SET_DELETE_TARGET_BUTTON_STATE,
 } from './constants';

export function deleteTarget(targetId, deleteTarget) {
  return {
    type: DELETE_TARGET,
    targetId,
    deleteTarget,
  }
}

export function targetDeleted(response) {
  return {
    type: DELETE_TARGET_SUCCESS,
    response,
  }
}

export function targetDeletedError(error) {
  return {
    type: DELETE_TARGET_ERROR,
    error,
  }
}

export function setDeleteTargetButtonState(enabled) {
  return {
    type: SET_DELETE_TARGET_BUTTON_STATE,
    enabled,
  }
}
