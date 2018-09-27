import React from 'react';
import PropTypes from 'prop-types';

import ReactLoading from 'react-loading';
import { formatDate, statusToObj } from 'utils/helper';


function LogsTableBody(props) {
  const { item, selectedItem } = props;
  const itemObj = statusToObj(item.status);
  let className;

  // Highlight selected row
  if (selectedItem && selectedItem.filename == item.filename) {
    className = `${className} table-active`
  }

  return (
    <tr className={className} onClick={() => props.onItemSelect(item.filename)}>
      <td />
      <td>{formatDate(item.timestamp)}</td>
      <td className={`text-center active ${itemObj.className}`}>
        <span>
          {item.status === "running"
          ? <span><ReactLoading type="bubbles" className="running-anim" />{itemObj.formattedMessage}</span>
          : itemObj.formattedMessage}
        </span>
      </td>
    </tr>
  );
}

LogsTableBody.propTypes = {
  item: PropTypes.object,
  selectedItem: PropTypes.any,
};

export default LogsTableBody;
