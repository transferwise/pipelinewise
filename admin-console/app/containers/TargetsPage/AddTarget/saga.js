import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  ADD_TARGET,
} from './constants';import {
  targetAdded,
  targetAddedError,
} from './actions';

import request from 'utils/request';

export function* addTarget(action) {
  const requestURL = `http://localhost:5000/add`;

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.newTarget)
    });
    yield put(targetAdded(response))
  } catch (err) {
    yield put(targetAddedError(err));
  }
}

export default function* targetData() {
  yield takeLatest(ADD_TARGET, addTarget);
}