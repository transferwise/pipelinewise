/*
 * TargetSnowflake Actions
 *
 * Actions change things in your application
 * Since this boilerplate uses a uni-directional data flow, specifically redux,
 * we have these actions which are the only way your application interacts with
 * your application state. This guarantees that your state is up to date and nobody
 * messes it up weirdly somewhere.
 *
 * To add a new Action:
 * 1) Import your constant
 * 2) Add a function like this:
 *    export function yourAction(var) {
 *        return { type: YOUR_ACTION_CONSTANT, var: var }
 *    }
 */

import {
  LOAD_CONFIG,
  LOAD_CONFIG_SUCCESS,
  LOAD_CONFIG_ERROR,

  SAVE_CONFIG,
  SAVE_CONFIG_SUCCESS,
  SAVE_CONFIG_ERROR,
  SET_SAVE_CONFIG_BUTTON_STATE,
 } from './constants';

export function loadConfig(targetId) {
  return {
    type: LOAD_CONFIG,
    targetId,
  }
}

export function configLoaded(config) {
  return {
    type: LOAD_CONFIG_SUCCESS,
    config,
  }
}

export function loadConfigError(error) {
  return {
    type: LOAD_CONFIG_ERROR,
    error,
  }
}

export function saveConfig(targetId, config) {
  return {
    type: SAVE_CONFIG,
    targetId,
    config
  }
}

export function saveConfigDone(response) {
  return {
    type: SAVE_CONFIG_SUCCESS,
    response,
  }
}

export function saveConfigError(error) {
  return {
    type: SAVE_CONFIG_ERROR,
    error,
  }
}

export function setSaveConfigButtonState(enabled) {
  return {
    type: SET_SAVE_CONFIG_BUTTON_STATE,
    enabled,
  }
}
