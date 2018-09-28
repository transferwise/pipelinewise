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
  makeSelectInheritableConfig,
  makeSelectLoading,
  makeSelectError,
  makeSelectSaving,
  makeSelectSavingError,
  makeSelectSavingSuccess,
  makeSelectSaveButtonEnabled,
} from './selectors';
import {
  loadInheritableConfig,
  saveInheritableConfig,
  setSaveButtonState,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: messages.title.defaultMessage,
  type: "object",
  properties: {
    schema: { type: "string", title: messages.schema.defaultMessage },
    "batch_size": { type: "integer", title: messages.batchSize.defaultMessage },
  },
}
const uiSchema = {
  schema: { "ui:help": messages.schemaHelp.defaultMessage },
  "batch_size": { "ui:help": messages.batchSizeHelp.defaultMessage },
};

export class TapInheritableConfig extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { inheritableConfig: undefined }
  }

  componentDidMount() {
    const { targetId, tapId } = this.props
    this.props.onLoadInheritableConfig(targetId, tapId);
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        inheritableConfig: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetDeleteButtonState(true)
    } else {
      this.props.onSetDeleteButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId, tapId } = this.props;
      this.props.onSaveInheritableConfig(targetId, tapId, this.state.inheritableConfig)
    }
  }

  render() {
    const {
      loading,
      error,
      saving,
      savingError,
      savingSuccess,
      inheritableConfig,
      saveButtonEnabled,
    } = this.props;
    let alert = <div />
    if (loading || saving) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }

    if (savingError !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {savingError.toString()}</Alert>
    }
    else if (savingSuccess) {
      alert = <Alert bsStyle="info" className="full-width"><strong>Saved!</strong></Alert>
    }

    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={this.state.inheritableConfig || inheritableConfig}
          showErrorList={false}
          liveValidate={true}
          onChange={(event) => this.onFormChange(event)}
          onSubmit={(event) => this.onFormSubmit(event)}
        >
          <Button bsStyle={saveButtonEnabled ? "danger" : "default"} type="submit" disabled={!saveButtonEnabled}><FormattedMessage {...messages.save} /></Button>
        </Form>
        <br />
        {alert}
      </Grid>
    )
  }
}

TapInheritableConfig.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  inheritableConfig: PropTypes.any,
  saveButtonEnabled: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadInheritableConfig: (targetId, tapId) => dispatch(loadInheritableConfig(targetId, tapId)),
    onSetDeleteButtonState: (enabled) => dispatch(setSaveButtonState(enabled)),
    onSaveInheritableConfig: (targetId, tapId, inheritableConfig) => dispatch(saveInheritableConfig(targetId, tapId, inheritableConfig)),
  };
}

const mapStateToProps = createStructuredSelector({
  inheritableConfig: makeSelectInheritableConfig(),
  loading: makeSelectLoading(),
  error: makeSelectError(),
  saving: makeSelectSaving(),
  savingError: makeSelectSavingError(),
  savingSuccess: makeSelectSavingSuccess(),
  saveButtonEnabled: makeSelectSaveButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapInheritableConfig', reducer });
const withSaga = injectSaga({ key: 'tapInheritableConfig', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapInheritableConfig);
