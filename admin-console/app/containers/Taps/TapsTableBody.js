import React from 'react';
import PropTypes from 'prop-types';
import Toggle from 'react-toggle';

import ConnectorIcon from 'components/ConnectorIcon';

function TapsTableBody(props) {
  const item = props.item;

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
      <td>{item.status}</td>
      <td>{item.lastSyncAt}</td>
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
