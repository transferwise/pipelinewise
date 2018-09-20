import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  ADD_TAP,
} from './constants';import {
  tapAdded,
  tapAddedError,
} from './actions';

import request from 'utils/request';

export function* addTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/add`;

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.newTap)
    });
    yield put(tapAdded(response))
  } catch (err) {
    yield put(tapAddedError(err));
  }
}

export default function* tapData() {
  yield takeLatest(ADD_TAP, addTap);
}