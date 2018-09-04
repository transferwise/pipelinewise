import { fromJS } from 'immutable';

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
} from './constants';

// The initial state of the App
const initialState = fromJS({
  targetsLoading: true,
  targetsError: false,
  targets: false,

  targetLoading: true,
  targetError: false,
  target: false,

  tapsLoading: true,
  tapsError: false,
  taps: false,

  tapLoading: true,
  tapError: false,
  tap: false,
});

function isValidTargets(targets) {
  return targets && Array.isArray(targets.result) && targets.result.length > 0
}

function appReducer(state = initialState, action) {
  let target;
  switch (action.type) {
    case LOAD_TARGETS:
      return state
        .set('targetsLoading', true)
        .set('targetsError', false)
        .set('targets', false)
        .set('target', false)
        .set('taps', false);
    case LOAD_TARGETS_SUCCESS:
      // Redirect to the first target if not specified in the URL
      if (isValidTargets(action.targets) && !action.selectedTargetId) {     
        window.location = `/targets/${action.targets.result[0].id}`;
      }
      
      return state
        .set('targetsLoading', false)
        .set('targetsError', action.targets.status !== 200 ? action.target.message : false)
        .set('targets', action.targets.result)
        .set('target', false)
        .set('taps', false);
    case LOAD_TARGETS_ERROR:
      return state
        .set('targetsLoading', false)
        .set('targetsError', action.error)
        .set('targets', false)
        .set('target', false)
        .set('taps', false);
    
    case LOAD_TARGET:
      return state
        .set('targetLoading', true)
        .set('targetError', false)
        .set('target', false)
        .set('taps', false)
    case LOAD_TARGET_SUCCESS:
      return state
        .set('targetLoading', false)
        .set('targetError', action.target.status !== 200 ? action.target.message : false)
        .set('tapsLoading', action.target.status !== 200 ? false: true)
        .set('tapsError', action.target.status !== 200 ? action.target.message : false)
        .set('target', action.target.result)
        .set('taps', false);
    case LOAD_TARGET_ERROR:
      return state
        .set('targetLoading', false)
        .set('targetError', action.error)
        .set('tapsLoading', false)
        .set('tapsError', action.error)
        .set('target', false)
        .set('taps', false);

    case LOAD_TAPS:
      return state
        .set('tapsLoading', true)
        .set('tapsError', false)
        .set('taps', false)
    case LOAD_TAPS_SUCCESS:
      return state
        .set('tapsLoading', false)
        .set('tapsError', action.taps.status !== 200 ? action.taps.message : false)
        .set('taps', action.taps.result)
    case LOAD_TAPS_ERROR:
      return state
        .set('tapsLoading', false)
        .set('tapsError', action.error)
        .set('taps', false)
    
    case LOAD_TAP:
      return state
        .set('tapLoading', true)
        .set('tapError', false)
        .set('tap', false)
    case LOAD_TAP_SUCCESS:
      return state
        .set('tapLoading', false)
        .set('tapError', action.tap.status !== 200 ? action.tap.message : false)
        .set('tap', action.tap.result)
    case LOAD_TAP_ERROR:
      return state
        .set('tapLoading', false)
        .set('tapError', action.error)
        .set('tap', false)

    default:
      return state;
  }
}

export default appReducer;
