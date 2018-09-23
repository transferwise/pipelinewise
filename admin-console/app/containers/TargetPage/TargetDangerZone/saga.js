import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  DELETE_TARGET,
} from './constants';import {
  targetDeleted,
  targetDeletedError,
} from './actions';

import request from 'utils/request';
import messages from './messages';

export function* deleteTarget(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/delete`;

  try {
    if (action.deleteTarget.id === action.targetId) {
      const response = yield call(request, requestURL, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
      });
      yield put(targetDeleted(response))
    } else {
      yield put(targetDeletedError(messages.targetToDeleteNotCorrect.defaultMessage))
    }
  } catch (err) {
    yield put(targetDeletedError(err));
  }
}

export default function* targetDangerZoneData() {
  yield takeLatest(DELETE_TARGET, deleteTarget);
}