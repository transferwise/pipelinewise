import React from 'react';
import PropTypes from 'prop-types';

function TransformationDropdown(props) {
  return (
    <select value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`transformation-straight-copy`} value="STRAIGHT_COPY">Straight Copy</option>
      <option key={`transformation-hash`} value="HASH">Hash</option>
      <option key={`transformation-setnull`} value="SET-NULL">Set to Null</option>
    </select>
  )
}

TransformationDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default TransformationDropdown;