import React from 'react';
import { FormattedMessage } from 'react-intl';
import PropTypes from 'prop-types';
import Toggle from 'react-toggle';

import ConnectorIcon from 'components/ConnectorIcon';
import { formatDate, statusToObj } from 'utils/helper';

function TapsTableBody(props) {
  const item = props.item;
  const currentStatusObj = statusToObj(item.status.currentStatus);
  const lastStatusObj = statusToObj(item.status.lastStatus);

  return (
    <tr>
      <td>
        <Toggle
          key={`tap-toggle-${item.id}`}
          defaultChecked={item.enabled}
          onChange={() => props.delegatedProps.onUpdateTapToReplicate(item.targetId, item.id, { update: { key: "enabled", value: !item.enabled }})}
        />
      </td>
      <td><a href={`/targets/${item.targetId}/taps/${item.id}`}><ConnectorIcon name={item.type} />&nbsp;<strong>{item.name}</strong></a></td>
      <td className={`text-center ${currentStatusObj.className}`}>{currentStatusObj.formattedMessage}</td>
      <td>{formatDate(item.status.lastTimestamp)}</td>
      <td className={`text-center ${lastStatusObj.className}`}>{lastStatusObj.formattedMessage}</td>
    </tr>
  );
}

TapsTableBody.propTypes = {
  target: PropTypes.any,
  item: PropTypes.object,
  selectedItem: PropTypes.any,
  onItemSelect: PropTypes.any,
};

export default TapsTableBody;
