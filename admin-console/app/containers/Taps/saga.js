import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TAPS, UPDATE_TAP_TO_REPLICATE } from 'containers/App/constants';
import {
  tapsLoaded,
  tapsLoadingError,
  updateTapToReplicateDone,
  updateTapToReplicateError,
} from 'containers/App/actions';

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

export function* updateTapToReplicate(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.params),
    });
    yield put(updateTapToReplicateDone(response));
  } catch (err) {
    yield put(updateTapToReplicateError(err));
  }
}

export default function* tapsData() {
  yield takeLatest(LOAD_TAPS, getTaps);
  yield takeLatest(UPDATE_TAP_TO_REPLICATE, updateTapToReplicate);
}
