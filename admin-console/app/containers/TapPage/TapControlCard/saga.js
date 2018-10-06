import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_TAP,
  SET_TAP_SYNC_PERIOD,
  RUN_TAP,
} from './constants';import {
  tapLoaded,
  loadTapError,
  setTapSyncPeriodDone,
  setTapSyncPeriodError,
  tapRunDone,
  tapRunError,
} from './actions';

import request from 'utils/request';

export function* getTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}`;

  try {
    const tap = yield call(request, requestURL);
    yield put(tapLoaded(tap));
  } catch (err) {
    yield put(loadTapError(err));
  }
}

export function* setTapSyncPeriod(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ update: { key: "sync_period", value: action.syncPeriod }}),
    });
    yield put(setTapSyncPeriodDone(response));
  } catch (err) {
    yield put(setTapSyncPeriodError(err));
  }
}

export function* runTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/run`;

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    yield put(tapRunDone(response))
  } catch (err) {
    yield put(tapRunError(err));
  }
}

export default function* tapControlCard() {
  yield takeLatest(LOAD_TAP, getTap);
  yield takeLatest(SET_TAP_SYNC_PERIOD, setTapSyncPeriod);
  yield takeLatest(RUN_TAP, runTap);
}