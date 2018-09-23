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
  makeSelectTargetLoading,
  makeSelectTargetError,
  makeSelectTarget
} from 'containers/App/selectors';

import { loadTarget } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';
import TargetTabbedContent from './TargetTabbedContent';

import { Grid, Row, Col, Alert } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class TargetPage extends React.PureComponent {
  componentDidMount() {
    const { match: { params } } = this.props;
    this.props.onLoadTarget(params.target);
  }

  renderHeader(target) {
    return (
      <Grid>
        <Row>
          <Col md={5} className="mt-1">
            <Row>
              <Col md={6} className="mt-2">
                <h4>{messages.target.defaultMessage}:</h4>
              </Col>
              <Col md={6}>
                <Row>
                  <ConnectorIcon name={target.type} />
                  <div>
                    <strong>{target.name}</strong>
                    <div><span className="text-muted"><FormattedMessage {...messages.targetType} />:</span> {target.type}</div>
                  </div>
                </Row>
              </Col>
            </Row>
          </Col>
        </Row>
      </Grid>
    )
  }

  render() {
    const { loading, error, target } = this.props;
    let alert = <div />;
    let content = <div />;

    if (loading) {
      return <LoadingIndicator />;
    }

    if (error != false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>
    } else {
      content = (
        <Grid>
          {this.renderHeader(target)}
          <hr />
          <TargetTabbedContent {... { target } } />
        </Grid>
      )
    }

    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Target</title>
        </Helmet>
        {alert}
        {content}
      </main>
    );
  }
}

TargetPage.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  target: PropTypes.any,
};

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTarget: (targetId) => dispatch(loadTarget(targetId)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTargetLoading(),
  error: makeSelectTargetError(),
  target: makeSelectTarget(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'target', reducer });
const withSaga = injectSaga({ key: 'target', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TargetPage);
