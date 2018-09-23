import React from 'react';
import { FormattedMessage } from 'react-intl';
import PropTypes from 'prop-types';
import Toggle from 'react-toggle';

import ConnectorIcon from 'components/ConnectorIcon';
import messages from './messages';

function statusToFormattedMessage(status) {
  let formattedMessage;

  switch (status) {
    case 'not-configured': formattedMessage = <FormattedMessage {...messages.statusNotConfigured} />
      break;
    case 'ready': formattedMessage = <FormattedMessage {...messages.statusReady} />
      break;
    default: formattedMessage = <FormattedMessage {...messages.statusUnknown} />
  }

  return formattedMessage
}

function TargetsTableBody(props) {
  const item = props.item;

  return (
    <tr>
      <td />
      <td><a href={`/targets/${item.id}`}><ConnectorIcon name={item.type} />&nbsp;<strong>{item.name}</strong></a></td>
      <td className={`text-center ${item.status === 'ready' ? '' : 'text-danger'}`}>{statusToFormattedMessage(item.status)}</td>
      <td>{item.lastSyncAt}</td>
    </tr>
  );
}

TargetsTableBody.propTypes = {
  target: PropTypes.any,
  item: PropTypes.object,
};

export default TargetsTableBody;
