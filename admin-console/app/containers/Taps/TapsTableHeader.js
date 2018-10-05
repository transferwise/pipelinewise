import React from 'react';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function TapsTableHeader() {
  return <tr>
    <th></th>
    <th><FormattedMessage {...messages.name} /></th>
    <th><FormattedMessage {...messages.owner} /></th>
    <th className="text-center"><FormattedMessage {...messages.status} /></th>
    <th className="text-center"><FormattedMessage {...messages.lastTimestamp} /></th>
    <th className="text-center"><FormattedMessage {...messages.lastStatus} /></th>
  </tr>;
}

export default TapsTableHeader;