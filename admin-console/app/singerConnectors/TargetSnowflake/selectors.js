/**
 * TargetPostgres selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTargetSnowflake = state => state.get('targetSnowflake', initialState);

const makeSelectConfig = () =>
  createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('config'));

const makeSelectForceReloadConfig = () =>
  createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('forceReloadConfig'))

const makeSelectLoading = () =>
  createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('savingSuccess'));

const makeSelectSaveConfigButtonEnabled = () =>
    createSelector(selectTargetSnowflake, targetSnowflakeState => targetSnowflakeState.get('saveConfigButtonEnabled'));

export {
  selectTargetSnowflake,
  
  makeSelectConfig,
  makeSelectForceReloadConfig,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveConfigButtonEnabled,
};
