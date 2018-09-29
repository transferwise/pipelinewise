import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function ReplicationMethodDropdown(props) {
  return (
    <select value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`replication-method-full`} value="FULL_TABLE"><FormattedMessage {...messages.full} /></option>
      <option key={`replication-method-incremental`} value="INCREMENTAL"><FormattedMessage {...messages.incremental} /></option>
      <option key={`replication-method-log-based`} value="LOG_BASED"><FormattedMessage {...messages.logBased} /></option>
    </select>
  )
}

ReplicationMethodDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default ReplicationMethodDropdown;