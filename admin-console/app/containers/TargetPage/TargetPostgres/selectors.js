/**
 * TargetPostgres selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTargetPostgres = state => state.get('targetPostgres', initialState);

const makeSelectConfig = () =>
  createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('forceReloadConfig'))

const makeSelectLoading = () =>
  createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTargetPostgres, targetPostgresState => targetPostgresState.get('saveConfigButtonEnabled'));

export {
  selectTargetPostgres,
  
  makeSelectConfig,
  makeSelectForceReloadConfig,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveConfigButtonEnabled,
};
