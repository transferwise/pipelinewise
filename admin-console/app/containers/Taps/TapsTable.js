import React from 'react';
import PropTypes from 'prop-types';

import { Alert, Grid } from 'react-bootstrap/lib';
import List from 'components/List';
import Table from 'components/Table';
import LoadingIndicator from 'components/LoadingIndicator';
import TapsTableHeader from './TapsTableHeader';
import TapsTableBody from './TapsTableBody';

function TapsTable({ loading, error, target, taps, tap, onTapSelect, onUpdateTapToReplicate }) {
  let items = [];
  let alert = <div />;
  let warning = <div />;

  if (loading) {
    return <List component={LoadingIndicator} />;
  }

  if (error != false) {
    alert = <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
  } else if (taps.length === 0) {
    warning = <Alert bsStyle="warning"><strong>Tip!</strong> No Integrations</Alert>
  } else {
    items = taps.map(t => Object.assign({ targetId: target.id }, t));
  }

  return (
    <Grid>
      <Table
        items={items}
        selectedItem={tap}
        headerComponent={TapsTableHeader}
        bodyComponent={TapsTableBody}
        onItemSelect={onTapSelect}
        delegatedProps={{ onUpdateTapToReplicate }}
      />
      {alert}
      {warning}
    </Grid>
  );
}

TapsTable.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  target: PropTypes.any,
  taps: PropTypes.any,
  tap: PropTypes.any,
  onTapSelect: PropTypes.func,
}

export default TapsTable;