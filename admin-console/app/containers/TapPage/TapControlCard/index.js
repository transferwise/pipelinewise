import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import { formatDate, statusToObj } from 'utils/helper';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';
import SyncPeriodDropdown from 'components/SyncPeriodDropDown';
import Modal from 'components/Modal';

import { FormattedMessage } from 'react-intl';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import IntervalTimer from 'react-interval-timer';
import { Grid, Row, Col, Alert, Button } from 'react-bootstrap/lib';
import ReactLoading from 'react-loading';

import {
  makeSelectTapLoading,
  makeSelectTapError,
  makeSelectTap,
  makeSelectRunTapLoading,
  makeSelectRunTapError,
  makeSelectRunTapSuccess,
  makeSelectRunTapButtonEnabled,
  makeSelectConsoleOutput,
} from './selectors';
import {
  loadTap,
  setRunTapButtonState,
  runTap,
  resetConsoleOutput,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';


export class TapControlCard extends React.PureComponent {
  componentDidMount() {
    const { targetId, tapId } = this.props
      this.props.onLoadTap(targetId, tapId);
  }

  onRefresh() {
    const { targetId, tapId } = this.props
    this.props.onLoadTap(targetId, tapId);
  }

  render() {
    const {
      tapLoading,
      tapError,
      tap,
      runTapLoading,
      runTapError,
      runTapSuccess,
      consoleOutput,
      onCloseModal,
    } = this.props;
    const targetId = tap && tap.target && tap.target.id;
    const tapId = tap && tap.id;
    const tapEnabled = tap && tap.enabled
    const tapStatus = tap && tap.status
    const tapCurrentStatus = tapStatus && tapStatus.currentStatus
    const tapLastStatus = tapStatus && tapStatus.lastStatus
    const tapLastTimestamp = tapStatus && tapStatus.lastTimestamp
    const tapSyncPeriod = tap && tap["sync_period"]

    const tapRunning = tapCurrentStatus === 'running'
    const runTapButtonEnabled = tap.enabled && tapCurrentStatus == 'ready' && !runTapSuccess;
    const currentStatusObj = statusToObj(runTapSuccess ? 'started' : tapCurrentStatus)
    const lastStatusObj = statusToObj(tapLastStatus)
    const lastTimestamp = tapLastTimestamp
    let alert = <div />
    let consolePanel = <div />

    if (tapLoading || runTapLoading) {
      return <LoadingIndicator />;
    }
    else if (tapError !== false) {
      return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {tapError.toString()}</Alert>;
    }
    else if (runTapError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {runTapError.toString()}</Alert>;
    }
    else if (runTapSuccess) {
      alert = <Alert bsStyle="success" className="full-width"><strong>Success!</strong> <FormattedMessage {...messages.runTapSuccess} /></Alert>;
    }

    if (consoleOutput !== false) {
      consolePanel = 
        <SyntaxHighlighter className="font-sssm" language='shsssell' style={light}
          showLineNumbers={false}>
            {consoleOutput}
        </SyntaxHighlighter> 
    }

    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        <h4>{messages.title.defaultMessage}</h4>
        <Row>
          <Col md={6}><ConnectorIcon name={tap.type} /></Col><Col md={6}><p><strong>{tap.name}</strong><br />(id: {tap.id})</p></Col>
          <Col md={6}><strong><FormattedMessage {...messages.tapOwner} />:</strong></Col><Col md={6}>{tap.owner || '-'}</Col>
          <Col md={6}><strong><FormattedMessage {...messages.target} />:</strong></Col><Col md={6}><a href={`/targets/${tap.target.id}`}>{tap.target.name}</a></Col>
          <Col md={6}><strong><FormattedMessage {...messages.status} />:</strong></Col><Col md={6} className={currentStatusObj.className}>
            {tapRunning
            ? <span>
                <ReactLoading type="bubbles" className="running-anim" />{currentStatusObj.formattedMessage}
                <IntervalTimer
                    timeout={10000}
                    callback={() => this.onRefresh()}
                    enabled={true}
                    repeat={true}
                  />
              </span>
            : currentStatusObj.formattedMessage}
          </Col>
          <Col md={12}><br /></Col>
          <Col md={6}><strong><FormattedMessage {...messages.lastTimestamp} />:</strong></Col><Col md={6}>{formatDate(lastTimestamp)}</Col>
          <Col md={6}><strong><FormattedMessage {...messages.lastStatus} />:</strong></Col><Col md={6} className={lastStatusObj.className}>{lastStatusObj.formattedMessage}</Col>
        </Row>
        <br />
        <Row>
          <Col md={6}><strong><FormattedMessage {...messages.syncPeriod} />:</strong></Col><Col md={6}><SyncPeriodDropdown value={tapSyncPeriod || -1} disabled={true} /></Col>
        </Row>
        <br /><br />
        <Row className="text-center">
          <Col md={12}>
            <Button bsStyle={runTapButtonEnabled ? "primary" : "default"} type="submit" disabled={!runTapButtonEnabled} onClick={() => this.props.onRunTap(targetId, tapId)}><FormattedMessage {...messages.runTap} /></Button>
          </Col>
        </Row>
        <br />
        {consoleOutput ?
          <Modal
            show={consoleOutput}
            title={<FormattedMessage {...messages.runTapError} />}
            body={<Grid>{alert}{consolePanel}</Grid>}
            onClose={() => onCloseModal()} />
        : alert }
      </Grid>
    )
  }
}

TapControlCard.propTypes = {
  tapLoading: PropTypes.bool,
  tapError: PropTypes.any,
  tap: PropTypes.any,
  runTapLoading: PropTypes.bool,
  runTapError: PropTypes.any,
  runTapSuccess: PropTypes.bool,
  success: PropTypes.any,
  runTapButtonEnabled: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadTap: (targetId, tapId) => dispatch(loadTap(targetId, tapId)),
    onSetRunTapButtonState: (enabled) => dispatch(setRunTapButtonState(enabled)),
    onRunTap: (targetId, tapId) => dispatch(runTap(targetId, tapId)),
    onCloseModal: () => dispatch(resetConsoleOutput())
  };
}

const mapStateToProps = createStructuredSelector({
  tapLoading: makeSelectTapLoading(),
  tapError: makeSelectTapError(),
  tap: makeSelectTap(),
  runTapLoading: makeSelectRunTapLoading(),
  runTapError: makeSelectRunTapError(),
  runTapSuccess: makeSelectRunTapSuccess(),
  consoleOutput: makeSelectConsoleOutput(),
  runTapButtonEnabled: makeSelectRunTapButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapControlCard', reducer });
const withSaga = injectSaga({ key: 'tapControlCard', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapControlCard);
