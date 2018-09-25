import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';
import Modal from 'components/Modal';

import { FormattedMessage } from 'react-intl';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import { Grid, Row, Col, Alert, Button } from 'react-bootstrap/lib';

import {
  makeSelectRunTapLoading,
  makeSelectRunTapError,
  makeSelectRunTapSuccess,
  makeSelectRunTapButtonEnabled,
  makeSelectConsoleOutput,
} from './selectors';
import {
  setRunTapButtonState,
  runTap,
  resetConsoleOutput,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';


export class TapControlCard extends React.PureComponent {


  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId, tapId } = this.props;
      this.props.onDeleteTap(targetId, tapId, this.state.tapToDelete)
    }
  }

  render() {
    const {
      runTapLoading,
      runTapError,
      runTapSuccess,
      tap,
      consoleOutput,
      onCloseModal,
    } = this.props;
    let alert = <div />
    let consolePanel = <div />
    const runTapButtonEnabled = tap.enabled && tap.status && tap.status == 'ready';
    const targetId = tap.target.id;
    const tapId = tap.id;

    if (runTapLoading) {
      return <LoadingIndicator />;
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
          <Col md={6}><strong><FormattedMessage {...messages.tapId} />:</strong></Col><Col md={6}>{tap.id}</Col>
          <Col md={6}><strong><FormattedMessage {...messages.tapName} />:</strong></Col><Col md={6}>{tap.name}</Col>
          <Col md={6}><strong><FormattedMessage {...messages.tapType} />:</strong></Col><Col md={6}><ConnectorIcon name={tap.type} /> {tap.type}</Col>
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
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  runTapButtonEnabled: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onSetRunTapButtonState: (enabled) => dispatch(setRunTapButtonState(enabled)),
    onRunTap: (targetId, tapId) => dispatch(runTap(targetId, tapId)),
    onCloseModal: () => dispatch(resetConsoleOutput())
  };
}

const mapStateToProps = createStructuredSelector({
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
