import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';

import { Helmet } from 'react-helmet';
import Toggle from 'react-toggle';
import { Grid, Alert, Row, Col } from 'react-bootstrap/lib';
import 'react-tabs/style/react-tabs.css';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';

import ArrowRightIcon from '../../images/arrow-right.png';

import {
  makeSelectTapLoading,
  makeSelectTapError,
  makeSelectTap,
  makeSelectForceRefreshTap,
} from 'containers/App/selectors';
import { loadTap, updateTapToReplicate } from '../App/actions';
import reducer from '../App/reducer';
import saga from './saga';
import TapTabbedContent from './TapTabbedContent';
import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export class TapPage extends React.PureComponent {
  componentDidMount() {
    const { match: { params } } = this.props;
    this.props.onLoadTap(params.target, params.tap);
  }

  componentDidUpdate(prevProps) {
    const { match: { params } } = this.props;
    if (this.props.forceRefreshTap) {
      this.props.onLoadTap(params.target, params.tap);
    }
  }
  
  renderHeader(tap, onUpdateTapToReplicate) {
    return (
      <Grid>
        <Row>
          <Col md={5} className="mt-1">
            <Row>
              <Col md={6} className="mt-2">
                <h4>{messages.tap.defaultMessage}:</h4>
              </Col>
              <Col md={6}>
                <Row>
                  <ConnectorIcon name={tap.type} />
                  <div>
                    <strong>{tap.name}</strong>
                    <div><span className="text-muted"><FormattedMessage {...messages.tapType} />:</span> {tap.type}</div>
                  </div>
                </Row>
              </Col>
            </Row>
          </Col>
          <Col md={1} className="mt-2">
            <Grid>
              <img className="img-icon" src={ArrowRightIcon} />
            </Grid>
          </Col>
          <Col md={5} className="mt-1">
            <Row>
              <Col md={6} className="mt-2">
                <h4>{messages.target.defaultMessage}:</h4>
              </Col>
              <Col md={6}>
                <Row className="float-right">
                  <ConnectorIcon name={tap.target.type} />
                  <div>
                    <a href={`/targets/${tap.target.id}`}><strong>{tap.target.name}</strong></a>
                    <div><span className="text-muted"><FormattedMessage {...messages.targetType} />:</span> {tap.target.type}</div>
                  </div>
                </Row>
              </Col>
            </Row>
          </Col>
          <Col md={1} className="mt-3">
            <Toggle
              defaultChecked={tap.enabled}
              className="float-right"
              onChange={() => onUpdateTapToReplicate(tap.target.id, tap.id, { update: { key: "enabled", value: !tap.enabled }})}
            />
          </Col>
        </Row>
      </Grid>
    )
  }

  render() {
    const { loading, error, tap, match, onUpdateTapToReplicate } = this.props;
    const targetId = match.params.target;
    let alert = <div />;
    let content = <div />;
    
    if (loading) {
      return <LoadingIndicator />;
    }

    if (error != false) {
      alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
    } else {
      content = (
        <div>
          {this.renderHeader(tap, onUpdateTapToReplicate)}
          <hr />
          <TapTabbedContent {... { targetId, tap } } />
        </div>
      )
    }
    
    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Tap</title>
        </Helmet>
        {alert}
        {content}
      </main>
    )
  }
}

TapPage.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  tap: PropTypes.any,
};

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTap: (targetId, tapId) => dispatch(loadTap(targetId, tapId)),
    onUpdateTapToReplicate: (targetId, tapId, params) => dispatch(updateTapToReplicate(targetId, tapId, params)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTapLoading(),
  error: makeSelectTapError(),
  tap: makeSelectTap(),
  forceRefreshTap: makeSelectForceRefreshTap(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tap', reducer });
const withSaga = injectSaga({ key: 'tap', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapPage);
