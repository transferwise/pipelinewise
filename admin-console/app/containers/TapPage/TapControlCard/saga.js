import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  RUN_TAP,
} from './constants';import {
  tapRunDone,
  tapRunError,
} from './actions';

import request from 'utils/request';

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
  yield takeLatest(RUN_TAP, runTap);
}