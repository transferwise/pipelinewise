import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';
import Modal from 'components/Modal';

import { FormattedMessage } from 'react-intl';
import { Grid, Alert, Button } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import Form from "react-jsonschema-form";
import {
  makeSelectConfig,
  makeSelectForceReloadConfig,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveConfigButtonEnabled,
  makeSelectTestingConnection,
  makeSelectTestingConnectionError,
  makeSelectTestingConnectionSuccess,
  makeSelectTestConnectionButtonEnabled,
  makeSelectConsoleOutput,
} from './selectors';
import {
  loadConfig,
  saveConfig,
  setSaveConfigButtonState,
  testConnection,
  setTestConnectionButtonState,
  resetConsoleOutput,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: "Connection",
  type: "object",
  properties: {
    access_token: { type: "string", title: "Access Token" },
    subdomain: { type: "string", title: "Subdomain" },
    start_date: { type: "string", title: "Start Date" }
  },
  required: ["access_token", "subdomain", "start_date"]
}

const uiSchema = {
  access_token: { "ui:widget": "password" },
};


export class TapS3Config extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { config: undefined }
  }

  componentDidMount() {
    const { targetId, tapId } = this.props
    this.props.onLoadConfig(targetId, tapId);
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        config: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetSaveConfigButtonState(true)
      this.props.onSetTestConnectionButtonState(false)
    } else {
      this.props.onSetSaveConfigButtonState(false)
      this.props.onSetTestConnectionButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId, tapId } = this.props;
      this.props.onSaveConfig(targetId, tapId, this.state.config)
    }
  }

  onTestConnection() {
    const { targetId, tapId } = this.props;
    this.props.onTestConnection(targetId, tapId, this.state.config)
  }

  render() {
    const {
      loading,
      error,
      title,
      config,
      saving,
      savingError,
      savingSuccess,
      saveConfigButtonEnabled,
      testingConnection,
      testingConnectionError,
      testingConnectionSuccess,
      testConnectionButtonEnabled,
      consoleOutput,
      onCloseModal,
    } = this.props;
    let alert = <div />
    let consolePanel = <div />

    if (loading || saving || testingConnection) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }

    if (savingError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong><FormattedMessage {...messages.saveConnectionError} /></strong> {savingError.toString()}</Alert>
    }
    else if (savingSuccess) {
      alert = <Alert bsStyle="info" className="full-width"><strong><FormattedMessage {...messages.saveConnectionSuccess} /></strong></Alert>
    }

    else if (testingConnectionError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong><FormattedMessage {...messages.testConnectionError} /></strong> {testingConnectionError.toString()}</Alert>
    }
    else if (testingConnectionSuccess) {
      alert = <Alert bsStyle="success" className="full-width"><strong><FormattedMessage {...messages.testConnectionSuccess} /></strong></Alert>
    }

    if (consoleOutput !== false) {
      consolePanel =
        <SyntaxHighlighter className="font-sssm" language='shsssell' style={light}
          showLineNumbers={false}>
            {consoleOutput}
        </SyntaxHighlighter>
    }

    schema.title = title || "Connection Details";
    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={this.state.config || config}
          showErrorList={false}
          liveValidate={true}
          onChange={(event) => this.onFormChange(event)}
          onSubmit={(event) => this.onFormSubmit(event)}
        >
          <Button bsStyle={saveConfigButtonEnabled ? "primary" : "default"} type="submit" disabled={!saveConfigButtonEnabled}><FormattedMessage {...messages.save} /></Button>
          &nbsp;
          <Button bsStyle={testConnectionButtonEnabled ? "success" : "default"} disabled={!testConnectionButtonEnabled} onClick={() => this.onTestConnection()}><FormattedMessage {...messages.testConnection} /></Button>
        </Form>
        <br />
        {consoleOutput ?
          <Modal
            show={consoleOutput}
            title={<FormattedMessage {...messages.testConnectionError} />}
            body={<Grid>{alert}{consolePanel}</Grid>}
            onClose={() => onCloseModal()} />
        : alert }
      </Grid>
    )
  }
}

TapS3Config.propTypes = {
  loading: PropTypes.any,
  error: PropTypes.any,
  title: PropTypes.any,
  targetId: PropTypes.any,
  tapId: PropTypes.any,
  config: PropTypes.any,
  forceReloadConfig: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadConfig: (targetId, tapId) => dispatch(loadConfig(targetId, tapId)),
    onSaveConfig: (targetId, tapId, config) => dispatch(saveConfig(targetId, tapId, config)),
    onSetSaveConfigButtonState: (enabled) => dispatch(setSaveConfigButtonState(enabled)),
    onTestConnection: (targetId, tapId, config) => dispatch(testConnection(targetId, tapId, config)),
    onSetTestConnectionButtonState: (enabled) => dispatch(setTestConnectionButtonState(enabled)),
    onCloseModal: () => dispatch(resetConsoleOutput())
  };
}

const mapStateToProps = createStructuredSelector({
  config: makeSelectConfig(),
  forceReloadConfig: makeSelectForceReloadConfig(),
  loading: makeSelectLoading(),
  error: makeSelectError(),
  saving: makeSelectSaving(),
  savingError: makeSelectSavingError(),
  savingSuccess: makeSelectSavingSuccess(),
  saveConfigButtonEnabled: makeSelectSaveConfigButtonEnabled(),
  testingConnection: makeSelectTestingConnection(),
  testingConnectionError: makeSelectTestingConnectionError(),
  testingConnectionSuccess: makeSelectTestingConnectionSuccess(),
  testConnectionButtonEnabled: makeSelectTestConnectionButtonEnabled(),
  consoleOutput: makeSelectConsoleOutput(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'TapS3', reducer });
const withSaga = injectSaga({ key: 'TapS3', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapS3Config);
