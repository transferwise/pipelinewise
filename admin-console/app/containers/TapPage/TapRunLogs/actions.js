import {
  LOAD_RUN_LOGS,
  LOAD_RUN_LOGS_SUCCESS,
  LOAD_RUN_LOGS_ERROR,

  LOAD_LOG_VIEWER,
  LOAD_LOG_VIEWER_SUCCESS,
  LOAD_LOG_VIEWER_ERROR,

  SET_LOG_VIEWER_VISIBLE,
  RESET_LOG_VIEWER,

  SET_ACTIVE_LOG_ID,
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

export function loadRunViewer(targetId, tapId, logId) {
  return {
    type: LOAD_LOG_VIEWER,
    targetId,
    tapId,
    logId,
  }
}

export function loadRunViewerDone(log) {
  return {
    type: LOAD_LOG_VIEWER_SUCCESS,
    log,
  }
}

export function loadRunViewerError(error) {
  return {
    type: LOAD_LOG_VIEWER_ERROR,
    error,
  }
}

export function setLogViewerVisible(visible) {
  return {
    type: SET_LOG_VIEWER_VISIBLE,
    visible,
  }
}

export function resetLogViewer() {
  return {
    type: RESET_LOG_VIEWER,
  }
}

export function setActiveLogId(logId) {
  return {
    type: SET_ACTIVE_LOG_ID,
    logId,
  }
}
