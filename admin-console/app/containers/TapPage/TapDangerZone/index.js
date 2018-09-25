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
  makeSelectTapToDelete,
  makeSelectDeleteTapButtonEnabled,
} from './selectors';
import {
  deleteTap,
  setDeleteTapButtonState,
} from './actions';
import reducer from './reducer';
import saga from './saga';

import messages from './messages';

const schema = {
  title: messages.title.defaultMessage,
  type: "object",
  properties: {
    id: { type: "string", title: messages.tapId.defaultMessage },
  },
  required: ["id"]
}
const uiSchema = {
  id: { "ui:help": messages.enterTapIdToDelete.defaultMessage }
};

export class TapDangerZone extends React.PureComponent {
  constructor(props) {
    super(props)
    this.state = { tapToDelete: undefined }
  }

  onFormChange(event) {
    if (event.errors.length === 0) {
      this.setState({
        tapToDelete: JSON.parse(JSON.stringify(event.formData)),
      });

      this.props.onSetDeleteTapButtonState(true)
    } else {
      this.props.onSetDeleteTapButtonState(false)
    }
  }

  onFormSubmit(event) {
    if (event.errors.length === 0) {
      const { targetId, tapId } = this.props;
      this.props.onDeleteTap(targetId, tapId, this.state.tapToDelete)
    }
  }

  render() {
    const {
      loading,
      error,
      success,
      tapToDelete,
      targetId,
      deleteTapButtonEnabled
    } = this.props;
    let alert = <div />

    if (loading) {
      return <LoadingIndicator />;
    }
    else if (error !== false) {
      alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>;
    }
    else if (success) {
      window.location.href = `/targets/${targetId}`;
    }

    return (
      <Grid className="shadow-sm p-3 mb-5 rounded">
        <Form
          schema={schema}
          uiSchema={uiSchema}
          formData={this.state.tapToDelete || tapToDelete}
          showErrorList={false}
          liveValidate={false}
          onChange={(event) => this.onFormChange(event)}
          onSubmit={(event) => this.onFormSubmit(event)}
          showErrorList={false}
        >
          <Button bsStyle={deleteTapButtonEnabled ? "danger" : "default"} type="submit" disabled={!deleteTapButtonEnabled}><FormattedMessage {...messages.delete} /></Button>
        </Form>
        <br />
        {alert}
      </Grid>
    )
  }
}

TapDangerZone.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  success: PropTypes.any,
  tapToDelete: PropTypes.any,
  deleteTapButtonEnabled: PropTypes.any,
}

export function mapDispatchToProps(dispatch) {
  return {
    onSetDeleteTapButtonState: (enabled) => dispatch(setDeleteTapButtonState(enabled)),
    onDeleteTap: (targetId, tapId, tapToDelete) => dispatch(deleteTap(targetId, tapId, tapToDelete)),
  };
}

const mapStateToProps = createStructuredSelector({
  loading: makeSelectLoading(),
  error: makeSelectError(),
  success: makeSelectSuccess(),
  tapToDelete: makeSelectTapToDelete(),
  deleteTapButtonEnabled: makeSelectDeleteTapButtonEnabled(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapDangerZone', reducer });
const withSaga = injectSaga({ key: 'tapDangerZone', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapDangerZone);
