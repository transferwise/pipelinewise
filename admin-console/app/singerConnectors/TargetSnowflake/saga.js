import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_CONFIG,
  SAVE_CONFIG,
  TEST_CONNECTION,
} from './constants';
import {
  configLoaded,
  loadConfigError,
  saveConfigDone,
  saveConfigError,
} from './actions';

import request from 'utils/request';

export function* loadConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/config`;

  try {
    const config = yield call(request, requestURL);
    yield put(configLoaded(config))
  } catch (err) {
    yield put(loadConfigError(err))
  }
}

export function* saveConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/config`

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.config)
    });
    yield put(saveConfigDone(response))
  } catch (err) {
    yield put(saveConfigError(err))
  }
}

export default function* targetSnowflakeData() {
  yield takeLatest(LOAD_CONFIG, loadConfig);
  yield takeLatest(SAVE_CONFIG, saveConfig);
}
