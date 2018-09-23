import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';

import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';

import { Helmet } from 'react-helmet';
import { FormattedMessage } from 'react-intl';
import { Grid, Alert, Row, Col, Button } from 'react-bootstrap/lib';
import Form from "react-jsonschema-form";

import {
  makeSelectNewTarget,
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,
  makeSelectAddTargetButtonEnabled,
} from './selectors';

import {
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
      enum: ["target-postgres"],
      enumNames: ["PostgreSQL"],
      default: "target-postgres",
    },
  },
  required: ["name", "type"]
}

const uiSchema = {};

/* eslint-disable react/prefer-stateless-function */
export class AddTargetPage extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { newTarget: undefined }
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
      window.location.href = `/targets/${newTarget.id}`;
    }

    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Add Target</title>
        </Helmet>
        <Grid>
          <Row>
            <Col md={2} />
            <Col md={8}>
              <Form
                schema={schema}
                uiSchema={uiSchema}
                formData={this.state.newTarget || newTarget}
                showErrorList={false}
                liveValidate={true}
                onChange={(event) => this.onFormChange(event)}
                onSubmit={(event) => this.onFormSubmit(event)}
              >
                <Button bsStyle={addTargetButtonEnabled ? "primary" : "default"} type="submit" disabled={!addTargetButtonEnabled}><FormattedMessage {...messages.add} /></Button>
              </Form>
              <br />
              {alert}
            </Col>
            <Col md={2} />
          </Row>
        </Grid>
      </main>
    )
  }
}

AddTargetPage.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  newTarget: PropTypes.any,
  addTargetButtonEnabled: PropTypes.any,
};

export function mapDispatchToProps(dispatch) {
  return {
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
)(AddTargetPage);

