import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';

import {
  makeSelectTapsLoading,
  makeSelectTapsError,
  makeSelectTarget,
  makeSelectTaps
} from 'containers/App/selectors';

import { loadTaps } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';

import { Grid, Col, Row } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import TapsTable from './TapsTable';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class Taps extends React.PureComponent {
  componentDidUpdate(prevProps) {
    const prevTargetId = prevProps.target && prevProps.target.id;
    const targetId = this.props.target && this.props.target.id;
    if (targetId && prevTargetId !== targetId) {
      this.props.onLoadTaps(targetId);
    }
  }
  
  render() {
    const { loading, error, target, taps, tap, onTapSelect } = this.props;
    const tapsTableProps = {
      loading,
      error,
      target,
      taps,
      tap,
      onTapSelect,
    };

    if (loading) {
      return <LoadingIndicator />;
    }

    return (
      <Grid>
        <strong><FormattedMessage {...messages.tapsTopic} /></strong>
        <br /><br />
        <Row>
          <Col md={12}>
            <TapsTable {...tapsTableProps} />
          </Col>
        </Row>
      </Grid>
    );
  }
}

Taps.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  target: PropTypes.any,
  targetId: PropTypes.any,
  taps: PropTypes.any,
  onLoadTaps: PropTypes.func,
  onTapSelect: PropTypes.func,
};

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTaps: targetId => dispatch(loadTaps(targetId)),
    onTapSelect: id => dispatch(setTap(id)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTapsLoading(),
  error: makeSelectTapsError(),
  target: makeSelectTarget(),
  taps: makeSelectTaps(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'taps', reducer });
const withSaga = injectSaga({ key: 'taps', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(Taps);
