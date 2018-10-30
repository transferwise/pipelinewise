import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';

import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';

import { FormattedMessage } from 'react-intl';
import { Grid, Alert, Row, Col, ButtonGroup, Button } from 'react-bootstrap/lib';
import Form from "react-jsonschema-form";

import {
  makeSelectNewTarget,
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,
  makeSelectAddTargetButtonEnabled,
} from './selectors';

import {
  setSuccess,
  setAddTargetButtonState,
  addTarget,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: messages.title.defaultMessage,
  type: "object",
  properties: {
    name: { type: "string", title: "Name" },
    type: {
      type: "string",
      title: "Data Warehouse Type",
      enum: ["target-postgres", "target-snowflake"],
      enumNames: ["PostgreSQL", "Snowflake"],
      default: "target-postgres",
    },
  },
  required: ["name", "type"]
}

const uiSchema = {};

/* eslint-disable react/prefer-stateless-function */
export class AddTarget extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { newTarget: undefined }
  }

  componentDidUpdate() {
    if (this.props.success && this.props.onSuccess) {
      this.props.onSuccess()
      this.props.onSetSuccess(false)
    }
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        newTarget: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetAddTargetButtonState(true)
    } else {
      this.props.onSetAddTargetButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      this.props.onAddTarget(this.state.newTarget)
    }
  }

  render() {
    const {
      loading,
      error,
      success,
      newTarget,
      addTargetButtonEnabled,
    } = this.props;
    let alert = <div />;
  
    if (loading) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }
    else if (success) {
      if (!this.props.onSuccess) {
        window.location.href = `/targets`;
      }
    }

    return (
      <Grid>
        <Row>
          <Col md={2} />
          <Col md={8} className="shadow-sm p-3 mb-5 rounded">
            <Form
              schema={schema}
              uiSchema={uiSchema}
              formData={this.state.newTarget || newTarget}
              showErrorList={false}
              liveValidate={true}
              onChange={(event) => this.onFormChange(event)}
              onSubmit={(event) => this.onFormSubmit(event)}
            >
              <ButtonGroup bsClass="float-right">
                {this.props.onCancel
                ? <Button bsStyle="warning" onClick={() => this.props.onCancel()}><FormattedMessage {...messages.cancel} /></Button>
                : <Grid />}
                &nbsp;
                <Button bsStyle={addTargetButtonEnabled ? "primary" : "default"} type="submit" disabled={!addTargetButtonEnabled}><FormattedMessage {...messages.add} /></Button>
              </ButtonGroup>
            </Form>
            <br /><br />
            {alert}
          </Col>
          <Col md={2} />
        </Row>
      </Grid>
    )
  }
}

AddTarget.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  newTarget: PropTypes.any,
  addTargetButtonEnabled: PropTypes.any,
  onSuccess: PropTypes.func,
  onCancel: PropTypes.func,
};

export function mapDispatchToProps(dispatch) {
  return {
    onSetSuccess: (success) => dispatch(setSuccess(success)),
    onSetAddTargetButtonState: (enabled) => dispatch(setAddTargetButtonState(enabled)),
    onAddTarget: (newTarget) => dispatch(addTarget(newTarget)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  success: makeSelectSuccess(),
  newTarget: makeSelectNewTarget(),
  addTargetButtonEnabled: makeSelectAddTargetButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'addTarget', reducer });
const withSaga = injectSaga({ key: 'addTarget', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(AddTarget);

