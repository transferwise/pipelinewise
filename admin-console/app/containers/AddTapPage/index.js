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
  makeSelectNewTap,
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,
  makeSelectAddTapButtonEnabled,
} from './selectors';

import {
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
      enum: ["tap-postgres"],
      enumNames: ["PostgreSQL"],
      default: "tap-postgres",
    },
  },
  required: ["name", "type"]
}

const uiSchema = {};

/* eslint-disable react/prefer-stateless-function */
export class AddTapPage extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { newTap: undefined }
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
      const { match } = this.props;
      const targetId = match.params.target;
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
      match
    } = this.props;
    let alert = <div />;
  
    if (loading) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }
    else if (success) {
      alert = <Alert bsStyle="info" className="full-width"><strong><FormattedMessage {...messages.addTapSuccess} /></strong></Alert>
    }

    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Add Tap</title>
        </Helmet>
        <Grid>
          <Row>
            <Col md={2} />
            {success
            ? <Col md={8} className="text-center">
                <br />
                <h4>{messages.addTapSuccess.defaultMessage}</h4>
                <Button bsStyle="primary" href={`/targets/${match.params.target}`}>Back</Button>
              </Col>
            : <Col md={8}>
                <Form
                  schema={schema}
                  uiSchema={uiSchema}
                  formData={this.state.newTap || newTap}
                  showErrorList={false}
                  liveValidate={true}
                  onChange={(event) => this.onFormChange(event)}
                  onSubmit={(event) => this.onFormSubmit(event)}
                >
                  <Button bsStyle={addTapButtonEnabled ? "primary" : "default"} type="submit" disabled={!addTapButtonEnabled}><FormattedMessage {...messages.add} /></Button>
                </Form>
                <br />
                {alert}
              </Col>}
            <Col md={2} />
          </Row>
        </Grid>
      </main>
    )
  }
}

AddTapPage.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  newTap: PropTypes.any,
  addTapButtonEnabled: PropTypes.any,
};

export function mapDispatchToProps(dispatch) {
  return {
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
)(AddTapPage);

