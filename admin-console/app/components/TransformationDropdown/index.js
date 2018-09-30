import React from 'react';
import PropTypes from 'prop-types';

function TransformationDropdown(props) {
  return (
    <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`transformation-straight-copy`} value="STRAIGHT_COPY">Straight Copy</option>
      <option key={`transformation-hash`} value="HASH">Hash</option>
      <option key={`transformation-setnull`} value="SET-NULL">Set to Null</option>
      <option key={`transformation-maskdate`} value="MASK-DATE">Mask Date</option>
      <option key={`transformation-masknumber`} value="MASK-NUMBER">Mask Number</option>
    </select>
  )
}

TransformationDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default TransformationDropdown;