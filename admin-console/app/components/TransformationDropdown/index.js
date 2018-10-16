import React from 'react';
import PropTypes from 'prop-types';

function TransformationDropdown(props) {
  return (
    <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`transformation-straight-copy`} value="STRAIGHT_COPY">Straight Copy</option>
      <option key={`transformation-hash`} value="HASH">Hash</option>
      <option key={`transformation-hash-skip-first-1-char`} value="HASH-SKIP-FIRST-1">Hash (Skip first character)</option>
      <option key={`transformation-hash-skip-first-2-char`} value="HASH-SKIP-FIRST-2">Hash (Skip first 2 characters)</option>
      <option key={`transformation-hash-skip-first-3-char`} value="HASH-SKIP-FIRST-3">Hash (Skip first 3 characters)</option>
      <option key={`transformation-hash-skip-first-4-char`} value="HASH-SKIP-FIRST-4">Hash (Skip first 4 characters)</option>
      <option key={`transformation-hash-skip-first-5-char`} value="HASH-SKIP-FIRST-5">Hash (Skip first 5 characters)</option>
      <option key={`transformation-hash-skip-first-6-char`} value="HASH-SKIP-FIRST-6">Hash (Skip first 6 characters)</option>
      <option key={`transformation-hash-skip-first-7-char`} value="HASH-SKIP-FIRST-7">Hash (Skip first 7 characters)</option>
      <option key={`transformation-hash-skip-first-8-char`} value="HASH-SKIP-FIRST-8">Hash (Skip first 8 characters)</option>
      <option key={`transformation-hash-skip-first-9-char`} value="HASH-SKIP-FIRST-9">Hash (Skip first 9 characters)</option>
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