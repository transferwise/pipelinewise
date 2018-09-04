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
  makeSelectTap
} from 'containers/App/selectors';
import { loadTap } from '../App/actions';
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
  
  renderHeader(tap) {
    return (
      <Grid>
        <Row>
          <Col md={4} className="mt-1">
            <Grid>
              <Row>
                <ConnectorIcon name={tap.type} />
                <div>
                  <strong>{tap.name}</strong>
                  <div><span className="text-muted">Tap Type:</span> {tap.type}</div>
                </div>
              </Row>
            </Grid>
          </Col>
          <Col md={1}>
            <Grid>
              <img className="img-icon" src={ArrowRightIcon} />
            </Grid>
          </Col>
          <Col md={4} className="mt-1">
            <Grid>
              <Row className="float-right">
                <ConnectorIcon name={tap.target.type} />
                <div>
                  <a href={`/targets/${tap.target.id}`}><strong>{tap.target.name}</strong></a>
                  <div><span className="text-muted">Target Type:</span> {tap.target.type}</div>
                </div>
              </Row>
            </Grid>
          </Col>
          <Col md={3} className="mt-3">
            <Toggle defaultChecked={tap.enabled} disabled={true} className="float-right"/>
          </Col>
        </Row>
      </Grid>
    )
  }

  render() {
    const { loading, error, tap, match } = this.props;
    const targetId = match.params.target;
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
          {this.renderHeader(tap)}
          <hr className="full-width" />
          <TapTabbedContent {... { targetId, tap } } />
        </Grid>
      )
    }
    
    return (
      <main role="main" className="container">
        <Helmet>
          <title>Tap</title>
        </Helmet>
        <Grid>
          {alert}
          {content}
        </Grid>
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
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectTapLoading(),
  error: makeSelectTapError(),
  tap: makeSelectTap(),
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
