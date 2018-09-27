import React from 'react';
import { FormattedMessage } from 'react-intl';
import dateFormat from 'dateformat';

import messages from './messages';

function formatDate(ts, format="yyyy-mm-dd hh:MM:ss") {
  try {
    return dateFormat(ts, format)
  } catch(err) {}

  return "Unknown"
}

function findItemByKey(a, k, m) {
  if (a && Array.isArray(a) && m) {
    return a.find(i => i[k] == m)
  }
  return false
}

function statusToObj(status) {
  let obj;

  switch (status) {
    case 'ready': obj = {
        className: 'text-success',
        color: '#5cb85c',
        message: messages.statusReady.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusReady} />,
      }
      break;
    case 'started': obj = {
        className: 'text-warning',
        color: '#f0ad4e',
        message: messages.statusStarted.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusStarted} />,
      }
      break;
    case 'running': obj = {
        className: 'text-primary',
        color: '#337ab7',
        message: messages.statusRunning.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusRunning} />,
      }
      break;
    case 'success': obj = {
        className: 'text-success',
        color: '#5cb85c',
        message: messages.statusSuccess.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusSuccess} />,
      }
      break;
    case 'failed': obj = {
        className: 'text-danger',
        color: '#d9534f',
        message: messages.statusFailed.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusFailed} />,
      }
      break;
    default: obj = {
        className: 'text-secondary font-italic',
        color: '#5bc0de',
        message: messages.statusUnknown.defaultMessage,
        formattedMessage: <FormattedMessage {...messages.statusUnknown} values={{ status }} />
      }
  }

  return obj
}

export {
  formatDate,
  findItemByKey,
  statusToObj,
}
