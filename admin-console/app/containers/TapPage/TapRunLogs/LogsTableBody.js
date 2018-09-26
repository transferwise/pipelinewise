import React from 'react';
import PropTypes from 'prop-types';

import { statusToObj } from 'utils/helper';


function LogsTableBody(props) {
  const { item, selectedItem } = props;
  const itemObj = statusToObj(item.status);
  let className;
  let createdAt;

  // Convert timestamp to local date string
  try {
    const dt = new Date(item.timestamp)
    createdAt = dt.toString()
  }
  catch(err) {
    createdAt = 'Unknown'
  }

  // Highlight selected row
  if (selectedItem && selectedItem.filename == item.filename) {
    className = `${className} table-active`
  }

  return (
    <tr className={className} onClick={() => props.onItemSelect(item.filename)}>
      <td />
      <td>{createdAt}</td>
      <td className={`text-center active ${itemObj.className}`}>{itemObj.formattedMessage}</td>
    </tr>
  );
}

LogsTableBody.propTypes = {
  item: PropTypes.object,
  selectedItem: PropTypes.any,
};

export default LogsTableBody;
