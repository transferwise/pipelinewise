import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TARGET } from 'containers/App/constants';
import { targetLoaded, targetLoadingError } from 'containers/App/actions';

import request from 'utils/request';

export function* getTarget(action) {
  const requestURL = `http://localhost:5000/targets/${action.id}`;

  try {
    const target = yield call(request, requestURL);
    yield put(targetLoaded(target));
  } catch (err) {
    yield put(targetLoadingError(err));
  }
}

export default function* targetData() {
  yield takeLatest(LOAD_TARGET, getTarget);
}
