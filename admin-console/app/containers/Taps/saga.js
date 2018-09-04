import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TAPS } from 'containers/App/constants';
import { tapsLoaded, tapsLoadingError } from 'containers/App/actions';

import request from 'utils/request';

export function* getTaps(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps`;

  try {
    const taps = yield call(request, requestURL);
    yield put(tapsLoaded(taps));
  } catch (err) {
    yield put(tapsLoadingError(err));
  }
}

export default function* tapsData() {
  yield takeLatest(LOAD_TAPS, getTaps);
}
