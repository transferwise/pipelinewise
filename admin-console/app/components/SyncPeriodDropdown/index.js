import React from 'react';
import PropTypes from 'prop-types';
import messages from './messages';

function SyncPeriodDropdown(props) {
  return (
    <select className="form-control" value={props.value} disabled={props.disabled} onChange={(event) => props.onChange(event.target.value)}>
      <option key={`sync-period-not-automated`} value="-1">{messages.syncPeriodNotAutomated.defaultMessage}</option>
      <option key={`sync-period-cron-5-mins`} value="*/5 * * * *">{messages.syncPeriodCron5Mins.defaultMessage}</option>
      <option key={`sync-period-cron-10-mins`} value="*/10 * * * *">{messages.syncPeriodCron10Mins.defaultMessage}</option>
      <option key={`sync-period-cron-15-mins`} value="*/15 * * * *">{messages.syncPeriodCron15Mins.defaultMessage}</option>
      <option key={`sync-period-cron-30-mins`} value="*/30 * * * *">{messages.syncPeriodCron30Mins.defaultMessage}</option>
      <option key={`sync-period-cron-1-hour`} value="0 * * * *">{messages.syncPeriodCronHourly.defaultMessage}</option>
      <option key={`sync-period-cron-2-hours`} value="0 */2 * * *">{messages.syncPeriodCron2Hours.defaultMessage}</option>
      <option key={`sync-period-cron-3-hours`} value="0 */3 * * *">{messages.syncPeriodCron3Hours.defaultMessage}</option>
      <option key={`sync-period-cron-6-hours`} value="0 */6 * * *">{messages.syncPeriodCron6Hours.defaultMessage}</option>
      <option key={`sync-period-cron-12-hours`} value="0 */12 * * *">{messages.syncPeriodCron12Hours.defaultMessage}</option>
      <option key={`sync-period-cron-daily`} value="0 0 * * *">{messages.syncPeriodCronDaily.defaultMessage}</option>
      <option key={`sync-period-cron-mon`} value="0 0 * * MON">{messages.syncPeriodCronEveryMonday.defaultMessage}</option>
      <option key={`sync-period-cron-tue`} value="0 0 * * TUE">{messages.syncPeriodCronEveryTuesday.defaultMessage}</option>
      <option key={`sync-period-cron-wed`} value="0 0 * * WED">{messages.syncPeriodCronEveryWednesday.defaultMessage}</option>
      <option key={`sync-period-cron-thu`} value="0 0 * * THU">{messages.syncPeriodCronEveryThursday.defaultMessage}</option>
      <option key={`sync-period-cron-fri`} value="0 0 * * FRI">{messages.syncPeriodCronEveryFriday.defaultMessage}</option>
      <option key={`sync-period-cron-sat`} value="0 0 * * SAT">{messages.syncPeriodCronEverySaturday.defaultMessage}</option>
      <option key={`sync-period-cron-sun`} value="0 0 * * SUN">{messages.syncPeriodCronEverySunday.defaultMessage}</option>
      <option key={`sync-period-cron-monthly-1-dom`} value="0 0 1 * *">{messages.syncPeriodCron1Mod.defaultMessage}</option>
      <option key={`sync-period-cron-monthly-2-dom`} value="0 0 2 * *">{messages.syncPeriodCron2Mod.defaultMessage}</option>
      <option key={`sync-period-cron-monthly-3-dom`} value="0 0 3 * *">{messages.syncPeriodCron3Mod.defaultMessage}</option>
      <option key={`sync-period-cron-monthly-4-dom`} value="0 0 4 * *">{messages.syncPeriodCron4Mod.defaultMessage}</option>
      <option key={`sync-period-cron-monthly-5-dom`} value="0 0 5 * *">{messages.syncPeriodCron5Mod.defaultMessage}</option>
    </select>
  )
}

SyncPeriodDropdown.propTypes = {
  value: PropTypes.any,
  disabled: PropTypes.any,
  onChange: PropTypes.func,
}

export default SyncPeriodDropdown;