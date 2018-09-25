import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';

import {
  makeSelectLoading,
  makeSelectError,
  makeSelectLogs,
} from './selectors';
import { loadRunLogs } from './actions';
import reducer from './reducer';
import saga from './saga';

import { Grid, Row, Col, Alert, ButtonGroup, Button } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';
import LogsTable from './LogsTable';
import messages from './messages';


export class TapRunLogs extends React.PureComponent {
  componentDidMount() {
    const { targetId, tapId } = this.props
    this.props.onLoadLogs(targetId, tapId);
  }

  onRefresh() {
    const { targetId, tapId } = this.props
    this.props.onLoadLogs(targetId, tapId);
  }

  render() {
    const { loading, error, logs } = this.props;
    const logsTableProps = {
      loading,
      error,
      logs,
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
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadLogs: (targetId, tapId) => dispatch(loadRunLogs(targetId, tapId)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  logs: makeSelectLogs(),
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
