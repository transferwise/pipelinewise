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
  makeSelectTaps,
  makeSelectForceRefreshTaps,
} from 'containers/App/selectors';

import { loadTaps, updateTapToReplicate } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';

import { Grid, Col, Row, ButtonGroup, Button } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import AddTap from './AddTap/Loadable';
import TapsTable from './TapsTable';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class Taps extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { addTapVisible: false }
  }

  componentDidMount() {
    const { targetId } = this.props;
    this.props.onLoadTaps(targetId);
  }

  componentDidUpdate(prevProps) {
    const prevTargetId = prevProps.target && prevProps.target.id;
    const targetId = this.props.target && this.props.target.id;
    if ((targetId && prevTargetId !== targetId) || this.props.forceRefreshTaps) {
      this.props.onLoadTaps(targetId);
    }
  }

  onAddTap() {
    this.setState({ addTapVisible: true });
  }

  onCancelAddTap() {
    this.setState({ addTapVisible: false });
  }

  onTapAdded() {
    this.setState({ addTapVisible: false })
    const targetId = this.props.target && this.props.target.id;
    if (targetId) {
      this.props.onLoadTaps(this.props.targetId)
    }
  }

  renderAddTapForm() {
    if (this.state.addTapVisible) {
      return (
        <Row>
          <Col md={12}></Col>
          <AddTap
            targetId={this.props.target.id}
            onSuccess={() => this.onTapAdded()}
            onCancel={() => this.onCancelAddTap()}/>
        </Row>
      )
    }
  }
  
  render() {
    const { loading, error, target, taps, tap, onTapSelect, onUpdateTapToReplicate } = this.props;
    const tapsTableProps = {
      loading,
      error,
      target,
      taps,
      tap,
      onTapSelect,
      onUpdateTapToReplicate
    };

    if (loading) {
      return <LoadingIndicator />;
    }

    return (
      <Grid>
        <h5>{messages.tapsTopic.defaultMessage}
          <ButtonGroup bsClass="float-right">
            <Button bsStyle="primary" onClick={() => this.onAddTap()}><FormattedMessage {...messages.addSource} /></Button>
          </ButtonGroup>
        </h5>
        <Row><Col md={12} /></Row>
        <br />
        {this.renderAddTapForm()}
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
  onUpdateTapToReplicate: PropTypes.func,
};

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTaps: targetId => dispatch(loadTaps(targetId)),
    onTapSelect: id => dispatch(setTap(id)),
    onUpdateTapToReplicate: (targetId, tapId, params) => dispatch(updateTapToReplicate(targetId, tapId, params)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTapsLoading(),
  error: makeSelectTapsError(),
  target: makeSelectTarget(),
  taps: makeSelectTaps(),
  forceRefreshTaps: makeSelectForceRefreshTaps(),
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
