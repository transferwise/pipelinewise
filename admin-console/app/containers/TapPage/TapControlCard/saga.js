import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_TAP,
  RUN_TAP,
} from './constants';import {
  tapLoaded,
  loadTapError,
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
  yield takeLatest(RUN_TAP, runTap);
}