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
  makeSelectNewTap,
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,
  makeSelectAddTapButtonEnabled,
} from './selectors';

import {
  setSuccess,
  setAddTapButtonState,
  addTap,
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
      title: "Data Source Type",
      enum: ["tap-postgres", "tap-mysql", "tap-zendesk", "tap-kafka", "tap-adwords", "tap-s3"],
      enumNames: ["PostgreSQL", "MySQL", "Zendesk", "Kafka", "Google Ads", "AWS S3"],
      default: "tap-postgres",
    },
    owner: { type: "string", title: "Owner" },
  },
  required: ["name", "type"]
}

const uiSchema = {
  owner: { "ui:help": "Team or email contact who is dealing with this data source."}
};

/* eslint-disable react/prefer-stateless-function */
export class AddTap extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { newTap: undefined }
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
        newTap: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetAddTapButtonState(true)
    } else {
      this.props.onSetAddTapButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const targetId = this.props.targetId;
      this.props.onAddTap(targetId, this.state.newTap)
    }
  }

  render() {
    const {
      loading,
      error,
      success,
      newTap,
      addTapButtonEnabled,
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
        window.location.href = `/targets/${newTap.target.id}`;
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
              formData={this.state.newTap || newTap}
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
                <Button bsStyle={addTapButtonEnabled ? "primary" : "default"} type="submit" disabled={!addTapButtonEnabled}><FormattedMessage {...messages.add} /></Button>
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

AddTap.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  targetId: PropTypes.any,
  newTap: PropTypes.any,
  addTapButtonEnabled: PropTypes.any,
  onSuccess: PropTypes.func,
  onCancel: PropTypes.func,
};

export function mapDispatchToProps(dispatch) {
  return {
    onSetSuccess: (success) => dispatch(setSuccess(success)),
    onSetAddTapButtonState: (enabled) => dispatch(setAddTapButtonState(enabled)),
    onAddTap: (targetId, newTap) => dispatch(addTap(targetId, newTap)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  success: makeSelectSuccess(),
  newTap: makeSelectNewTap(),
  addTapButtonEnabled: makeSelectAddTapButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'addTap', reducer });
const withSaga = injectSaga({ key: 'addTap', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(AddTap);

