import React from 'react';
import { FormattedMessage } from 'react-intl';
import PropTypes from 'prop-types';

import messages from './messages';

function statusToObj(status) {
  let obj;

  switch (status) {
    case 'running': obj = {
        className: 'text-primary',
        formattedMessage: <FormattedMessage {...messages.statusRunning} />,
      }
      break;
    case 'success': obj = {
        className: 'text-success',
        formattedMessage: <FormattedMessage {...messages.statusSuccess} />,
      }
      break;
    case 'failed': obj = {
        className: 'text-danger',
        formattedMessage: <FormattedMessage {...messages.statusFailed} />,
      }
      break;
    default: obj = {
        formattedMessage: <FormattedMessage {...messages.statusUnknown} />
      }
  }

  return obj
}

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
