import React from 'react';
import PropTypes from 'prop-types';

function SyncPeriodDropdown(props) {
  return (
    <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`sync-period-0`} value="-1">Not automated</option>
      <option key={`sync-period-5`} value="5">5 minutes</option>
      <option key={`sync-period-10`} value="10">10 minutes</option>
      <option key={`sync-period-15`} value="15">15 minutes</option>
      <option key={`sync-period-30`} value="30">30 minutes</option>
      <option key={`sync-period-60`} value="60">1 hour</option>
      <option key={`sync-period-120`} value="120">2 hours</option>
      <option key={`sync-period-180`} value="180">3 hours</option>
      <option key={`sync-period-360`} value="360">6 hours</option>
      <option key={`sync-period-720`} value="720">12 hours</option>
      <option key={`sync-period-1440`} value="1440">1 day</option>
      <option key={`sync-period-10080`} value="10080">7 days</option>
    </select>
  )
}

SyncPeriodDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default SyncPeriodDropdown;