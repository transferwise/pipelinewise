import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';

import {
  makeSelectTargetsLoading,
  makeSelectTargetsError,
  makeSelectTargets
} from 'containers/App/selectors';

import { loadTargets } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';

import { Grid, Alert } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class Targets extends React.PureComponent {
  static redirectNewTarget(targetId) {
    window.location = `/targets/${targetId}`;
  }

  componentDidMount() {
    this.props.onLoadTargets(this.props.selectedTargetId);
  }

  renderDropdown(targets, selectedTargetId) {
    if (Array.isArray(targets) && targets.length > 0) {
      const options = targets.map((target, i) => (
        <option key={`target-${target.id}`} value={target.id}>{target.name}</option>
      ));
    
      return (
        <select value={selectedTargetId} onChange={(event) => Targets.redirectNewTarget(event.target.value)}>
          {options}
        </select>
      );
    }

    return <div />;
  }

  render() {
    const { loading, error, targets, selectedTargetId } = this.props;
    let alert = <div />;
    let warning = <div />;

    if (loading) {
      return <LoadingIndicator />;
    }

    if (error != false) {
      alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
    } else {
      if (targets.length === 0) {
        warning = <Alert bsStyle="warning"><strong>Tip!</strong> No Integrations</Alert>
      }
    }

    return (
      <Grid>
        <strong><FormattedMessage {...messages.header} /></strong>
        <br /><br />
        {this.renderDropdown(targets, selectedTargetId)}
        {alert}
        {warning}
      </Grid>
    );
  }
}


Targets.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  targets: PropTypes.any,
  selectedTargetId: PropTypes.any,
  onLoadTargets: PropTypes.func,
};

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTargets: selectedTargetId => dispatch(loadTargets(selectedTargetId)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTargetsLoading(),
  error: makeSelectTargetsError(),
  targets: makeSelectTargets(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'targets', reducer });
const withSaga = injectSaga({ key: 'targets', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(Targets);
