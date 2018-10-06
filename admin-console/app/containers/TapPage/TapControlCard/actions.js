import {
  LOAD_TAP,
  LOAD_TAP_SUCCESS,
  LOAD_TAP_ERROR,

  SET_TAP_SYNC_PERIOD,
  SET_TAP_SYNC_PERIOD_SUCCESS,
  SET_TAP_SYNC_PERIOD_ERROR,

  RUN_TAP,
  RUN_TAP_SUCCESS,
  RUN_TAP_ERROR,

  RESET_CONSOLE_OUTPUT,

  SET_RUN_TAP_BUTTON_STATE,
 } from './constants';

export function loadTap(targetId, tapId) {
  return {
    type: LOAD_TAP,
    targetId,
    tapId,
  }
}

export function tapLoaded(tap) {
  return {
    type: LOAD_TAP_SUCCESS,
    tap,
  }
}

export function loadTapError(error) {
  return {
    type: LOAD_TAP_ERROR,
    error,
  }
}

export function setTapSyncPeriod(targetId, tapId, syncPeriod) {
  return {
    type: SET_TAP_SYNC_PERIOD,
    targetId,
    tapId,
    syncPeriod,
  }
}

export function setTapSyncPeriodDone(response) {
  return {
    type: SET_TAP_SYNC_PERIOD_SUCCESS,
    response,
  }
}

export function setTapSyncPeriodError(error) {
  return {
    type: SET_TAP_SYNC_PERIOD_ERROR,
    error,
  }
}

export function runTap(targetId, tapId) {
  return {
    type: RUN_TAP,
    targetId,
    tapId,
  }
}

export function tapRunDone(response) {
  return {
    type: RUN_TAP_SUCCESS,
    response,
  }
}

export function tapRunError(error) {
  return {
    type: RUN_TAP_ERROR,
    error,
  }
}

export function resetConsoleOutput() {
  return {
    type: RESET_CONSOLE_OUTPUT,
  }
}

export function setRunTapButtonState(enabled) {
  return {
    type: SET_RUN_TAP_BUTTON_STATE,
    enabled,
  }
}
