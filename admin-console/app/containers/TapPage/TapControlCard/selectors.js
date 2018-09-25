/**
 * TapControlCard selectors
 */

import { createSelector } from 'reselect';
import { initialState } from './reducer';

const selectTapControlCard = state => state.get('tapControlCard', initialState);

const makeSelectRunTapLoading = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapLoading'));

const makeSelectRunTapError = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapError'));

const makeSelectRunTapSuccess = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapSuccess'));

const makeSelectConsoleOutput = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('consoleOutput'));

const makeSelectRunTapButtonEnabled = () =>
  createSelector(selectTapControlCard, TapControlCardState => TapControlCardState.get('runTapButtonEnabled'));

export {
  selectTapControlCard,
  
  makeSelectRunTapLoading,
  makeSelectRunTapError,
  makeSelectRunTapSuccess,
  makeSelectConsoleOutput,

  makeSelectRunTapButtonEnabled,
};
