import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TAP, UPDATE_TAP_TO_REPLICATE } from 'containers/App/constants';
import { tapLoaded, tapLoadingError } from 'containers/App/actions';

import request from 'utils/request';
import { updateTapToReplicateDone, updateTapToReplicateError } from '../App/actions';

export function* getTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}`;

  try {
    const tap = yield call(request, requestURL);
    yield put(tapLoaded(tap));
  } catch (err) {
    yield put(tapLoadingError(err));
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

export default function* tapData() {
  yield takeLatest(LOAD_TAP, getTap);
  yield takeLatest(UPDATE_TAP_TO_REPLICATE, updateTapToReplicate);
}
