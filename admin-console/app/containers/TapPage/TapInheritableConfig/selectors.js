/**
 * TapInheritableConfig selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapInheritableConfig = state => state.get('tapInheritableConfig', initialState);

const makeSelectInheritableConfig = () =>
  createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('inheritableConfig'));

const makeSelectLoading = () =>
  createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('loading'));

const makeSelectError = () =>
  createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('error'));

const makeSelectSaving = () =>
  createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('saving'));

const makeSelectSavingError = () =>
    createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('savingError'));

const makeSelectSavingSuccess = () =>
    createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('savingSuccess'));

const makeSelectSaveButtonEnabled = () =>
    createSelector(selectTapInheritableConfig, tapInheritableConfigState => tapInheritableConfigState.get('saveButtonEnabled'));

export {
  makeSelectInheritableConfig,
  
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveButtonEnabled,
};
