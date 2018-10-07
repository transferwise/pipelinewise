import React from 'react';
import PropTypes from 'prop-types';
import messages from './messages';

const prettyCron = require('prettycron');

function SyncPeriodDropdown(props) {
  const easySchedules = {
    "-1": { name: messages.syncPeriodNotAutomated.defaultMessage },
    "*/5 * * * *": { name: messages.syncPeriodCron5Mins.defaultMessage },
    "*/10 * * * *": { name: messages.syncPeriodCron10Mins.defaultMessage },
    "*/15 * * * *": { name: messages.syncPeriodCron15Mins.defaultMessage },
    "*/30 * * * *": { name: messages.syncPeriodCron30Mins.defaultMessage },
    "0 * * * *": { name: messages.syncPeriodCronHourly.defaultMessage },
    "0 */2 * * *": { name: messages.syncPeriodCron2Hours.defaultMessage },
    "0 */3 * * *": { name: messages.syncPeriodCron3Hours.defaultMessage },
    "0 */6 * * *": { name: messages.syncPeriodCron6Hours.defaultMessage },
    "0 */12 * * *": { name: messages.syncPeriodCron12Hours.defaultMessage },
    "0 0 * * *": { name: messages.syncPeriodCronDaily.defaultMessage },
    "0 0 * * MON": { name: messages.syncPeriodCronEveryMonday.defaultMessage },
    "0 0 * * TUE": { name: messages.syncPeriodCronEveryTuesday.defaultMessage },
    "0 0 * * WED": { name: messages.syncPeriodCronEveryWednesday.defaultMessage },
    "0 0 * * THU": { name: messages.syncPeriodCronEveryThursday.defaultMessage },
    "0 0 * * FRI": { name: messages.syncPeriodCronEveryFriday.defaultMessage },
    "0 0 * * SAT": { name: messages.syncPeriodCronEverySaturday.defaultMessage },
    "0 0 * * SUN": { name: messages.syncPeriodCronEverySunday.defaultMessage },
    "0 0 1 * *": { name: messages.syncPeriodCron1Mod.defaultMessage },
    "0 0 2 * *": { name: messages.syncPeriodCron2Mod.defaultMessage },
    "0 0 3 * *": { name: messages.syncPeriodCron3Mod.defaultMessage },
    "0 0 4 * *": { name: messages.syncPeriodCron4Mod.defaultMessage },
    "0 0 5 * *": { name: messages.syncPeriodCron5Mod.defaultMessage },
  }
  if (easySchedules[props.value]) {
    return (
      <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
        {Object.keys(easySchedules).map((k, i) => <option key={`sync-period-${i}`} value={k}>{easySchedules[k].name}</option>)}
      </select>
    )
  } else {
    return (
      <div>
        <select className="form-control" value={props.value} disabled={true} onChange={(event) => props.onChange(event.target.value)}>
          <option key="sync-period-custom" value={props.value}>{props.value}</option>)}
        </select>
        {prettyCron.toString(props.value)}
      </div>
    )
  }
}

SyncPeriodDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default SyncPeriodDropdown;