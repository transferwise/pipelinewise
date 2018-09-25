import React from 'react';
import { FormattedMessage } from 'react-intl';
import PropTypes from 'prop-types';

import ConnectorIcon from 'components/ConnectorIcon';
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
  const item = props.item;
  const itemObj = statusToObj(item.status);
  let createdAt;

  try {
    const dt = new Date(item.timestamp)
    createdAt = dt.toString()
  }
  catch(err) {
    createdAt = 'Unknown'
  }

  return (
    <tr>
      <td />
      <td>{createdAt}</td>
      <td className={`text-center ${itemObj.className}`}>{itemObj.formattedMessage}</td>
    </tr>
  );
}

LogsTableBody.propTypes = {
  item: PropTypes.object,
};

export default LogsTableBody;
