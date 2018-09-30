import React from 'react';
import PropTypes from 'prop-types';

import messages from './messages';

function ReplicationMethodDropdown(props) {
  return (
    <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`replication-method-full`} value="FULL_TABLE">{messages.full.defaultMessage}</option>
      <option key={`replication-method-incremental`} value="INCREMENTAL">{messages.incremental.defaultMessage}</option>
      <option key={`replication-method-log-based`} value="LOG_BASED">{messages.logBased.defaultMessage}</option>
    </select>
  )
}

ReplicationMethodDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default ReplicationMethodDropdown;