import React from 'react';
import { Redirect } from 'react-router-dom'
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
import TargetsTable from './TargetsTable';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class TargetsPage extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { redirectToAddTarget: false }
  }

  componentDidMount() {
    this.props.onLoadTargets(this.props.selectedTargetId);
  }

  onAddTarget() {
    this.setState({ redirectToAddTarget: true });
  }

  renderRedirectToAddTarget() {
    if (this.state.redirectToAddTarget) {
      return <Redirect to={`/add`} />
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
          {this.renderRedirectToAddTarget()}

          <h5>{messages.header.defaultMessage}
            <ButtonGroup bsClass="float-right">
              <Button bsStyle="primary" onClick={() => this.onAddTarget()}><FormattedMessage {...messages.addTarget} /></Button>
            </ButtonGroup>
          </h5>
          <Row>
            <Col md={12}>
            </Col>
          </Row>

          <br />

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
