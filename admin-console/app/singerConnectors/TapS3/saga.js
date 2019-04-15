import { call, put, select, takeLatest } from 'redux-saga/effects';
import {
  LOAD_STREAMS,
  LOAD_CONFIG,
  SAVE_CONFIG,
  TEST_CONNECTION,
  UPDATE_STREAMS,
  UPDATE_STREAM,
  SET_TRANSFORMATION,
  DISCOVER_TAP
} from './constants';
import {
  streamsLoaded,
  streamsLoadedError,
  configLoaded,
  loadConfigError,
  saveConfigDone,
  saveConfigError,
  updateStreamsDone,
  updateStreamsError,
  updateStreamDone,
  updateStreamError,
  setTransformationDone,
  setTransformationError,
  discoverTapDone,
  discoverTapError,
  testConnectionSucces,
  testConnectionError,
} from './actions';

import request from 'utils/request';

export function* getStreams(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/streams`;

  try {
    const streams = yield call(request, requestURL);
    yield put(streamsLoaded(streams));
  } catch (err) {
    yield put(streamsLoadedError(err));
  }
}

export function* loadConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/config`;

  try {
    const config = yield call(request, requestURL);
    yield put(configLoaded(config))
  } catch (err) {
    yield put(loadConfigError(err))
  }
}

export function* saveConfig(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/config`

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.config)
    });
    yield put(saveConfigDone(response))
  } catch (err) {
    yield put(saveConfigError(err))
  }
}

export function* testConnection(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/testconnection`

  try {
    const response = yield call(request, requestURL);
    yield put(testConnectionSucces(response))
  } catch (err) {
    yield put(testConnectionError(err))
  }
}

export function* updateStreams(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/streams`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.params),
    });
    yield put(updateStreamsDone(response));
  } catch (err) {
    yield put(updateStreamsError(err));
  }
}

export function* updateStream(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/streams/${action.streamId}`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(action.params),
    });
    yield put(updateStreamDone(response));
  } catch (err) {
    yield put(updateStreamError(err));
  }
}

export function* setTransformation(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/transformations/${action.stream}/${action.fieldId}`;

  try {
    const response = yield call(request, requestURL, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: action.value })
    })
    yield put(setTransformationDone(response));
  } catch (err) {
    yield put(setTransformationError(err));
  }
}

export function* discoverTap(action) {
  const requestURL = `http://localhost:5000/targets/${action.targetId}/taps/${action.tapId}/discover`;

  try {
    const response = yield call(request, requestURL, {
      method: 'POST',
    });
    yield put(discoverTapDone(response));
  } catch (err) {
    yield put(discoverTapError(err));
  }
}

export default function* tapS3Data() {
  yield takeLatest(LOAD_STREAMS, getStreams);
  yield takeLatest(LOAD_CONFIG, loadConfig);
  yield takeLatest(SAVE_CONFIG, saveConfig);
  yield takeLatest(TEST_CONNECTION, testConnection);
  yield takeLatest(UPDATE_STREAMS, updateStreams);
  yield takeLatest(UPDATE_STREAM, updateStream);
  yield takeLatest(SET_TRANSFORMATION, setTransformation)
  yield takeLatest(DISCOVER_TAP, discoverTap);
}
