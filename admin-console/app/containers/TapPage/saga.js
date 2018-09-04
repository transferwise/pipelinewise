import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TAP } from 'containers/App/constants';
import { tapLoaded, tapLoadingError } from 'containers/App/actions';

import request from 'utils/request';

export function* getTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}`;

  try {
    const tap = yield call(request, requestURL);
    yield put(tapLoaded(tap));
  } catch (err) {
    yield put(tapLoadingError(err));
  }
}

export default function* tapData() {
  yield takeLatest(LOAD_TAP, getTap);
}
