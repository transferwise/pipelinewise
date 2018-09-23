import React from 'react';
import PropTypes from 'prop-types';

import { Alert, Grid } from 'react-bootstrap/lib';
import List from 'components/List';
import Table from 'components/Table';
import LoadingIndicator from 'components/LoadingIndicator';
import TargetsTableHeader from './TargetsTableHeader';
import TargetsTableBody from './TargetsTableBody';

function TargetsTable({ loading, error, targets }) {
  let items = [];
  let alert = <div />;
  let warning = <div />;
  console.log(targets)

  if (loading) {
    return <List component={LoadingIndicator} />;
  }

  if (error != false) {
    alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
  } else if (targets.length === 0) {
    warning = <Alert bsStyle="warning"><strong>Tip!</strong> No Destinations</Alert>
  }

  return (
    <Grid>
      <Table
        items={targets}
        headerComponent={TargetsTableHeader}
        bodyComponent={TargetsTableBody}
      />
      {alert}
      {warning}
    </Grid>
  );
}

TargetsTable.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  targets: PropTypes.any,
}

export default TargetsTable;