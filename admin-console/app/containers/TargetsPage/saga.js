import { call, put, select, takeLatest } from 'redux-saga/effects';
import { LOAD_TARGETS } from 'containers/App/constants';
import { targetsLoaded, targetsLoadingError } from 'containers/App/actions';

import request from 'utils/request';

export function* getTargets(action) {
  const requestURL = `http://localhost:5000/targets`;

  try {
    const targets = yield call(request, requestURL);
    yield put(targetsLoaded(targets, action.selectedTargetId));
  } catch (err) {
    yield put(targetsLoadingError(err));
  }
}

export default function* targetData() {
  yield takeLatest(LOAD_TARGETS, getTargets);
}
