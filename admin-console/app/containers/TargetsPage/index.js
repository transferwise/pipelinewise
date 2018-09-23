import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';

import { Helmet } from 'react-helmet';
import {
  makeSelectTargetsLoading,
  makeSelectTargetsError,
  makeSelectTargets
} from 'containers/App/selectors';

import { loadTargets } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';

import { Grid, Row, Col, Alert, ButtonGroup, Button } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import AddTarget from './AddTarget/Loadable';
import TargetsTable from './TargetsTable';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class TargetsPage extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { addTargetVisible: false }
  }

  componentDidMount() {
    this.props.onLoadTargets(this.props.selectedTargetId);
  }

  onAddTarget() {
    this.setState({ addTargetVisible: true });
  }

  onCancelAddTarget() {
    this.setState({ addTargetVisible: false });
  }

  onTargetAdded() {
    this.setState({ addTargetVisible: false })
    this.props.onLoadTargets(this.props.selectedTargetId)
  }

  renderAddTargetForm() {
    if (this.state.addTargetVisible) {
      return (
        <Row>
          <Col md={12}></Col>
          <AddTarget
            onSuccess={() => this.onTargetAdded()}
            onCancel={() => this.onCancelAddTarget()}/>
        </Row>
      )
    }
  }

  render() {
    const { loading, error, targets } = this.props;
    const targetsTableProps = {
      loading,
      error,
      targets,
    };
    let alert = <div />;
    let content = <div />;

    if (loading) {
      return <LoadingIndicator />;
    }

    if (error != false) {
      alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
    } else {
      content = (
        <Grid>
          <h5>{messages.header.defaultMessage}
            <ButtonGroup bsClass="float-right">
              <Button bsStyle="primary" onClick={() => this.onAddTarget()}><FormattedMessage {...messages.addTarget} /></Button>
            </ButtonGroup>
          </h5>
          <Row><Col md={12} /></Row>
          <br />
          {this.renderAddTargetForm()}
          <Row>
            <Col md={12}>
              <TargetsTable {...targetsTableProps} />
            </Col>
          </Row>
        </Grid>
      )
    }

    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Targets</title>
        </Helmet>
        {alert}
        {content}
      </main>
    )
  }
}

TargetsPage.propTypes = {
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
)(TargetsPage);
