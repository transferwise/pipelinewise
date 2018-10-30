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
  makeSelectConfig,
  makeSelectForceReloadConfig,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveConfigButtonEnabled,
} from './selectors';
import {
  loadConfig,
  saveConfig,
  setSaveConfigButtonState,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: "Connection",
  type: "object",
  properties: {
    account: { type: "string", title: "Snowflake Account Name" },
    user: { type: "string", title: "User" },
    password: { type: "string", title: "Password" },
    dbname: { type: "string", title: "Database name" },
    schema: { type: "string", title: "Schema to use" },
    warehouse: { type: "string", title: "Virtual Warehouse" },
    s3_url: { type: "string", title: "URL to External Stage on S3" },
    aws_access_key_id: { type: "string", title: "AWS Access Key ID" },
    aws_secret_access_key: { type: "string", title: "AWS Secret Access Key" },
  },
  required: ["account", "user", "password", "dbname", "schema", "warehouse", "s3_url", "aws_access_key_id", "aws_secret_access_key"]
}

const uiSchema = {
  password: { "ui:widget": "password" },
};


export class TargetSnowflakeConfig extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { config: undefined }
  }
  
  componentDidMount() {
    const { targetId } = this.props
    this.props.onLoadConfig(targetId);
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        config: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetSaveConfigButtonState(true)
    } else {
      this.props.onSetSaveConfigButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId } = this.props;
      this.props.onSaveConfig(targetId, this.state.config)
    }
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
    } = this.props;
    let alert = <div />
    let consolePanel = <div />

    if (loading || saving) {
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

    schema.title = title || "Connection Details";
    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        {alert}
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
        </Form>
      </Grid>
    )
  }
}

TargetSnowflakeConfig.propTypes = {
  loading: PropTypes.any,
  error: PropTypes.any,
  title: PropTypes.any,
  targetId: PropTypes.any,
  config: PropTypes.any,
  forceReloadConfig: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadConfig: (targetId) => dispatch(loadConfig(targetId)),
    onSaveConfig: (targetId, config) => dispatch(saveConfig(targetId, config)),
    onSetSaveConfigButtonState: (enabled) => dispatch(setSaveConfigButtonState(enabled))
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
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'targetSnowflake', reducer });
const withSaga = injectSaga({ key: 'targetSnowflake', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TargetSnowflakeConfig);
