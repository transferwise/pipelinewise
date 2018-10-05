import React from 'react';
import { FormattedMessage } from 'react-intl';
import PropTypes from 'prop-types';
import Toggle from 'react-toggle';
import ReactLoading from 'react-loading';
import Moment from 'react-moment';
import HumanizeDuration from 'humanize-duration';

import ConnectorIcon from 'components/ConnectorIcon';
import { statusToObj } from 'utils/helper';

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
      <td>{item['sync_period'] ? HumanizeDuration(item['sync_period'] * 60000) : <p class="font-italic">Not Automated</p>}</td>
      <td className={`text-center ${currentStatusObj.className}`}>
        <span>
          {item.status.currentStatus === "running"
          ? <span><ReactLoading type="bubbles" className="running-anim" />{currentStatusObj.formattedMessage}</span>
          : currentStatusObj.formattedMessage}
        </span>
      </td>
      <td>{item.status.lastTimestamp ? <span><Moment fromNow ago>{item.status.lastTimestamp}</Moment> ago</span>: '-'}</td>
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
