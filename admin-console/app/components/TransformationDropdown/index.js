import React from 'react';
import PropTypes from 'prop-types';

function TransformationDropdown(props) {
  return (
    <select value={props.value} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`transformation-straight-copy`} value="STRAIGHT_COPY">Straight Copy</option>
      <option key={`transformation-hash`} value="HASH">Hash</option>
    </select>
  )
}

TransformationDropdown.propTypes = {
  target: PropTypes.any,
  value: PropTypes.any,
  onChange: PropTypes.func,
}

export default TransformationDropdown;