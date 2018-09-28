import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_INHERITABLE_CONFIG,
  SAVE_INHERITABLE_CONFIG,
} from './constants';
import {
  inheritableConfigLoaded,
  loadInheritableConfigError,
  saveInheritableConfigDone,
  saveInheritableConfigError,
} from './actions';

import request from 'utils/request';

export function* getInheritableConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/inheritableconfig`;

  try {
    const inheritableConfig = yield call(request, requestURL);
    yield put(inheritableConfigLoaded(inheritableConfig))
  } catch (err) {
    yield put(loadInheritableConfigError(err))
  }
}

export function* saveInheritableConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/inheritableconfig`

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.inheritableConfig)
    });
    yield put(saveInheritableConfigDone(response))
  } catch (err) {
    yield put(saveInheritableConfigError(err))
  }
}

export default function* tapInheritableConfigData() {
  yield takeLatest(LOAD_INHERITABLE_CONFIG, getInheritableConfig);
  yield takeLatest(SAVE_INHERITABLE_CONFIG, saveInheritableConfig);
}
