import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';

import { FormattedMessage } from 'react-intl';
import { Grid, Alert, Button } from 'react-bootstrap/lib';
import Form from "react-jsonschema-form";
import {
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectTestingConnection,
  makeSelectTestingConnectionError
} from './selectors';
import {
  saveConfig,
  testConnection,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: "Connection",
  type: "object",
  properties: {
    host: { type: "string", title: "Host" },
    port: { type: "integer", title: "Port" },
    user: { type: "string", title: "User" },
    password: { type: "string", title: "Password" },
    dbname: { type: "string", title: "Database name" },
    filter_dbs: { type: "string", title: "Filter Databases" },
  },
  required: ["host", "port", "user", "password", "dbname"]
}

const uiSchema = {
  password: { "ui:widget": "password" },
};


export class TapPostgresConfig extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = {
      saveConfigButtonEnabled: false,
      testConnectionButtonEnabled: true,
      config: props.config,
    }
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        saveConfigButtonEnabled: true,
        testConnectionButtonEnabled: true,
        config: JSON.parse(JSON.stringify(event.formData)),
      });
    } else {
      this.setState({
        saveConfigButtonEnabled: false,
        testConnectionButtonEnabled: false,
      });
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const tap = this.props.tap;
      if (tap) {
        this.props.onSaveConfig(tap.target.id, tap.id, this.state.config)
      }
    }
  }

  onTestConnection() {
    const tap = this.props.tap;
    this.props.onTestConnection(tap.target.id, tap.id, this.state.config)
  }

  render() {
    const { saving, savingError, testingConnection, testingConnectionError, tap } = this.props;
    let alert = <div />

    if (saving || testingConnection) {
      return <LoadingIndicator />;
    }

    if (savingError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error when saving!</strong> {savingError.toString()}</Alert>
    }

    if (testingConnectionError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Cannot test connection!</strong> {testingConnectionError.toString()}</Alert>
    }

    schema.title = `${tap.name} Connection`;
    return (
      <Grid>
        {alert}
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={this.state.config}
          showErrorList={false}
          liveValidate={true}
          onChange={(event) => this.onFormChange(event)}
          onSubmit={(event) => this.onFormSubmit(event)}
        >
          <Button bsStyle="primary" type="submit" disabled={!this.state.saveConfigButtonEnabled}><FormattedMessage {...messages.save} /></Button>
          &nbsp;
          <Button bsStyle="success" disabled={!this.state.testConnectionButtonEnabled} onClick={() => this.onTestConnection()}><FormattedMessage {...messages.testConnection} /></Button>
        </Form>
      </Grid>
    )
  }
}

TapPostgresConfig.propTypes = {
  tap: PropTypes.any,
  config: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onSaveConfig: (targetId, tapId, config) => dispatch(saveConfig(targetId, tapId, config)),
    onTestConnection: (targetId, tapId, config) => dispatch(testConnection(targetId, tapId, config)),
  };
}

const mapStateToProps = createStructuredSelector({
  saving: makeSelectSaving(),
  savingError: makeSelectSavingError(),
  testingConnection: makeSelectTestingConnection(),
  testingConnectionError: makeSelectTestingConnectionError(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapPostgres', reducer });
const withSaga = injectSaga({ key: 'tapPostgres', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapPostgresConfig);
