import {
  LOAD_TARGETS,
  LOAD_TARGETS_SUCCESS,
  LOAD_TARGETS_ERROR,

  LOAD_TARGET,
  LOAD_TARGET_SUCCESS,
  LOAD_TARGET_ERROR,

  LOAD_TAPS,
  LOAD_TAPS_SUCCESS,
  LOAD_TAPS_ERROR,

  LOAD_TAP,
  LOAD_TAP_SUCCESS,
  LOAD_TAP_ERROR,

  UPDATE_TAP_TO_REPLICATE,
  UPDATE_TAP_TO_REPLICATE_SUCCESS,
  UPDATE_TAP_TO_REPLICATE_ERROR,
} from './constants';

export function loadTargets(selectedTargetId) {
  return {
    type: LOAD_TARGETS,
    selectedTargetId,
  };
}

export function targetsLoaded(targets, selectedTargetId) {
  return {
    type: LOAD_TARGETS_SUCCESS,
    targets,
    selectedTargetId,
  };
}

export function targetsLoadingError(error) {
  return {
    type: LOAD_TARGETS_ERROR,
    error,
  };
}

export function loadTarget(targetId) {
  return {
    type: LOAD_TARGET,
    targetId,
  };
}

export function targetLoaded(target) {
  return {
    type: LOAD_TARGET_SUCCESS,
    target,
  }
}

export function targetLoadingError(error) {
  return {
    type: LOAD_TARGET_ERROR,
    error,
  }
}

export function loadTaps(targetId) {
  return {
    type: LOAD_TAPS,
    targetId,
  };
}

export function tapsLoaded(taps) {
  return {
    type: LOAD_TAPS_SUCCESS,
    taps,
  };
}

export function tapsLoadingError(error) {
  return {
    type: LOAD_TAPS_ERROR,
    error,
  };
}

export function loadTap(targetId, tapId) {
  return {
    type: LOAD_TAP,
    targetId,
    tapId,
  };
}
export function tapLoaded(tap) {
  return {
    type: LOAD_TAP_SUCCESS,
    tap,
  };
}

export function tapLoadingError(error) {
  return {
    type: LOAD_TAP_ERROR,
    error,
  };
}

export function updateTapToReplicate(targetId, tapId, params) {
  return {
    type: UPDATE_TAP_TO_REPLICATE,
    targetId,
    tapId,
    params,
  };
}

export function updateTapToReplicateDone(response) {
  return {
    type: UPDATE_TAP_TO_REPLICATE_SUCCESS,
    response,
  };
}

export function updateTapToReplicateError(error) {
  return {
    type: UPDATE_TAP_TO_REPLICATE_ERROR,
    error,
  };
}
