import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_RUN_LOGS,
  LOAD_LOG_VIEWER,
} from './constants';import {
  loadRunLogsDone,
  loadRunLogsError,

  loadRunViewerDone,
  loadRunViewerError,
} from './actions';

import request from 'utils/request';

export function* logTapRunLogs(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/logs`;

  try {
    const logs = yield call(request, requestURL)
    yield put(loadRunLogsDone(logs))
  } catch (err) {
    yield put(loadRunLogsError(err));
  }
}

export function* logTapRunLog(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/logs/${action.logId}`;

  try {
    const logs = yield call(request, requestURL)
    yield put(loadRunViewerDone(logs))
  } catch (err) {
    yield put(loadRunViewerError(err));
  }
}

export default function* tapRunLogs() {
  yield takeLatest(LOAD_RUN_LOGS, logTapRunLogs);
  yield takeLatest(LOAD_LOG_VIEWER, logTapRunLog)
}