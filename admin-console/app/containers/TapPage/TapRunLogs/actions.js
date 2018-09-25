import {
  LOAD_RUN_LOGS,
  LOAD_RUN_LOGS_SUCCESS,
  LOAD_RUN_LOGS_ERROR,
 } from './constants';

export function loadRunLogs(targetId, tapId) {
  return {
    type: LOAD_RUN_LOGS,
    targetId,
    tapId,
  }
}

export function loadRunLogsDone(logs) {
  return {
    type: LOAD_RUN_LOGS_SUCCESS,
    logs,
  }
}

export function loadRunLogsError(error) {
  return {
    type: LOAD_RUN_LOGS_ERROR,
    error,
  }
}
