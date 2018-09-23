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
  makeSelectLoading,
  makeSelectError,
  makeSelectSuccess,
  makeSelectTargetToDelete,
  makeSelectDeleteTargetButtonEnabled,
} from './selectors';
import {
  deleteTarget,
  setDeleteTargetButtonState,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: messages.title.defaultMessage,
  type: "object",
  properties: {
    id: { type: "string", title: messages.targetId.defaultMessage },
  },
  required: ["id"]
}
const uiSchema = {
  id: { "ui:help": messages.enterTargetIdToDelete.defaultMessage }
};

export class TargetDangerZone extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { targetToDelete: undefined }
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        targetToDelete: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetDeleteTargetButtonState(true)
    } else {
      this.props.onSetDeleteTargetButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId } = this.props;
      this.props.onDeleteTarget(targetId, this.state.targetToDelete)
    }
  }

  render() {
    const {
      loading,
      error,
      success,
      targetToDelete,
      targetId,
      deleteTargetButtonEnabled
    } = this.props;
    let alert = <div />

    if (loading) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }
    else if (success) {
      window.location.href = `/targets`;
    }

    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        {alert}
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={this.state.targetToDelete || targetToDelete}
          showErrorList={false}
          liveValidate={false}
          onChange={(event) => this.onFormChange(event)}
          onSubmit={(event) => this.onFormSubmit(event)}
          showErrorList={false}
        >
          <Button bsStyle={deleteTargetButtonEnabled ? "danger" : "default"} type="submit" disabled={!deleteTargetButtonEnabled}><FormattedMessage {...messages.delete} /></Button>
        </Form>
      </Grid>
    )
  }
}

TargetDangerZone.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  targetToDelete: PropTypes.any,
  deleteTargetButtonEnabled: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onSetDeleteTargetButtonState: (enabled) => dispatch(setDeleteTargetButtonState(enabled)),
    onDeleteTarget: (targetId, targetToDelete) => dispatch(deleteTarget(targetId, targetToDelete)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  success: makeSelectSuccess(),
  targetToDelete: makeSelectTargetToDelete(),
  deleteTargetButtonEnabled: makeSelectDeleteTargetButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'targetDangerZone', reducer });
const withSaga = injectSaga({ key: 'targetDangerZone', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TargetDangerZone);
