import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import { findItemByKey, timestampToFormattedString, statusToObj } from 'utils/helper';

import {
  makeSelectLoading,
  makeSelectError,
  makeSelectLogs,
  makeSelectViewerLoading,
  makeSelectViewerError,
  makeSelectLog,
  makeSelectLogViewerVisible,
  makeSelectActiveLogId,
} from './selectors';
import {
  loadRunLogs,
  setActiveLogId,
  loadRunViewer,
  resetLogViewer
} from './actions';
import reducer from './reducer';
import saga from './saga';

import { Grid, Row, Col, Alert, ButtonGroup, Button } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import Modal from 'components/Modal';
import LogsTable from './LogsTable';
import messages from './messages';


export class TapRunLogs extends React.PureComponent {
  componentDidMount() {
    const { targetId, tapId } = this.props
    this.props.onLoadLogs(targetId, tapId);
  }

  componentDidUpdate(prevProps) {
    const { targetId, tapId, activeLogId } = this.props;
    if (activeLogId != prevProps.activeLogId) {
      this.props.onLoadLogViewer(targetId, tapId, activeLogId)
    }
  }

  onRefresh() {
    const { targetId, tapId } = this.props
    this.props.onLoadLogs(targetId, tapId);
  }

  renderLogViewer() {
    const { logs, activeLogId, viewerLoading, viewerError, log, logViewerVisible } = this.props
    const activeLog = findItemByKey(logs, 'filename', activeLogId)
    const itemObj = statusToObj(activeLog.status);
    const createdAt = timestampToFormattedString(activeLog.timestamp);
    let alert = <div />
    let logContent = <div />

    const viewerHeader = (
      <Grid>
        <Row>
          <Col md={4}><FormattedMessage {...messages.createdAt} /></Col><Col md={8}>{createdAt}</Col>
          <Col md={4}><FormattedMessage {...messages.status} /></Col><Col md={8} className={itemObj.className}>{itemObj.formattedMessage}</Col>
        </Row>
      </Grid>
    )

    if (viewerLoading) {
      logContent =  <LoadingIndicator />
    }
    else {
      if (viewerError) {
        alert = <Alert bsStyle="danger"><strong>Error!</strong> {viewerError.toString()}</Alert>
      }
      else {
        logContent = (
          <SyntaxHighlighter className="font-sssm" language='shsssell' style={light} showLineNumbers={false}>
              {log || '<EMPTY>'}
          </SyntaxHighlighter>
        );
      }
    }

    return (
      <Modal
        show={logViewerVisible}
        title={<FormattedMessage {...messages.logViewerTitle} />}
        body={<Grid>{viewerHeader}<br />{alert}{logContent}</Grid>}
        onClose={() => this.props.onCloseLogViewer()} />
      )
  }

  render() {
    const {
      loading,
      error,
      logs,
      activeLogId,
      onLogSelect
    } = this.props;
    const activeLog = findItemByKey(logs, 'filename', activeLogId)
    const logsTableProps = {
      loading,
      error,
      logs,
      activeLog,
      onLogSelect,
    };
    let alert = <div />

    if (loading) {
      return <LoadingIndicator />;
    }
    
    if (error != false) {
      alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
    }

    return (
      <Grid>
        <h5>{messages.title.defaultMessage}
          <ButtonGroup bsClass="float-right">
            <Button bsStyle="primary" onClick={() => this.onRefresh()}><FormattedMessage {...messages.refresh} /></Button>
          </ButtonGroup>
        </h5>
        <br />
        {alert}
        <Row>
          <Col md={12}>
            <LogsTable {...logsTableProps} />
          </Col>
        </Row>
        {this.renderLogViewer()}
      </Grid>
    )
  }
}

TapRunLogs.propTypes = {
  targetId: PropTypes.any,
  tapId: PropTypes.any,
  loading: PropTypes.bool,
  error: PropTypes.any,
  logs: PropTypes.any,
  viewerLoading: PropTypes.any,
  viewerError: PropTypes.any,
  log: PropTypes.any,
  logViewerVisible: PropTypes.any,
  activeLogId: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadLogs: (targetId, tapId) => dispatch(loadRunLogs(targetId, tapId)),
    onLogSelect: (logId) => dispatch(setActiveLogId(logId)),
    onLoadLogViewer: (targetId, tapId, logId) => dispatch(loadRunViewer(targetId, tapId, logId)),
    onCloseLogViewer: () => dispatch(resetLogViewer())
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  logs: makeSelectLogs(),
  viewerLoading: makeSelectViewerLoading(),
  viewerError: makeSelectViewerError(),
  log: makeSelectLog(),
  logViewerVisible: makeSelectLogViewerVisible(),
  activeLogId: makeSelectActiveLogId(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapRunLogs', reducer });
const withSaga = injectSaga({ key: 'tapRunLogs', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapRunLogs);
