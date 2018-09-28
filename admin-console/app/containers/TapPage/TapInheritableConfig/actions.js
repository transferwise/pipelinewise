import {
  LOAD_INHERITABLE_CONFIG,
  LOAD_INHERITABLE_CONFIG_SUCCESS,
  LOAD_INHERITABLE_CONFIG_ERROR,

  SAVE_INHERITABLE_CONFIG,
  SAVE_INHERITABLE_CONFIG_SUCCESS,
  SAVE_INHERITABLE_CONFIG_ERROR,

  SET_SAVE_BUTTON_STATE,
 } from './constants';

export function loadInheritableConfig(targetId, tapId) {
  return {
    type: LOAD_INHERITABLE_CONFIG,
    targetId,
    tapId,
  }
}

export function inheritableConfigLoaded(inheritableConfig) {
  return {
    type: LOAD_INHERITABLE_CONFIG_SUCCESS,
    inheritableConfig,
  }
}

export function loadInheritableConfigError(error) {
  return {
    type: LOAD_INHERITABLE_CONFIG_ERROR,
    error,
  }
}

export function saveInheritableConfig(targetId, tapId, inheritableConfig) {
  return {
    type: SAVE_INHERITABLE_CONFIG,
    targetId,
    tapId,
    inheritableConfig,
  }
}

export function saveInheritableConfigDone(response) {
  return {
    type: SAVE_INHERITABLE_CONFIG_SUCCESS,
    response,
  }
}

export function saveInheritableConfigError(error) {
  return {
    type: SAVE_INHERITABLE_CONFIG_ERROR,
    error,
  }
}

export function setSaveButtonState(enabled) {
  return {
    type: SET_SAVE_BUTTON_STATE,
    enabled,
  }
}
