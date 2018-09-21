import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  DELETE_TAP,
} from './constants';import {
  tapDeleted,
  tapDeletedError,
} from './actions';

import request from 'utils/request';
import messages from './messages';

export function* deleteTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/delete`;

  try {
    if (action.deleteTap.id === action.tapId) {
      const response = yield call(request, requestURL, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
      });
      yield put(tapDeleted(response))
    } else {
      yield put(tapDeletedError(messages.tapToDeleteNotCorrect.defaultMessage))
    }
  } catch (err) {
    yield put(tapDeletedError(err));
  }
}

export default function* tapDangerZoneData() {
  yield takeLatest(DELETE_TAP, deleteTap);
}