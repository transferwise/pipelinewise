import React from 'react';
import PropTypes from 'prop-types';

import { Alert, Grid } from 'react-bootstrap/lib';
import List from 'components/List';
import Table from 'components/Table';
import LoadingIndicator from 'components/LoadingIndicator';
import LogsTableHeader from './LogsTableHeader';
import LogsTableBody from './LogsTableBody';
import messages from './messages';

function LogsTable({ loading, error, logs }) {
  let alert = <div />;
  let warning = <div />;

  if (loading) {
    return <List component={LoadingIndicator} />;
  }

  if (error != false) {
    alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
  } else if (logs.length === 0) {
    warning = <Alert bsStyle="warning"><strong>Tip!</strong> {messages.noLog.defaultMessage}</Alert>
  }

  // Sort logs by timestamps descentive
  logs.sort((a, b) => a.timestamp < b.timestamp ? 1 : -1)

  return (
    <Grid>
      <Table
        items={logs}
        headerComponent={LogsTableHeader}
        bodyComponent={LogsTableBody}
      />
      {alert}
      {warning}
    </Grid>
  );
}

LogsTable.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  logs: PropTypes.any,
}

export default LogsTable;