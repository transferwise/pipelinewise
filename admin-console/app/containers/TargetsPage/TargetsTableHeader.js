import React from 'react';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function TargetsTableHeader() {
  return <tr>
    <th></th>
    <th><FormattedMessage {...messages.name} /></th>
    <th className="text-center"><FormattedMessage {...messages.status} /></th>
    <th className="text-center"><FormattedMessage {...messages.lastSyncAt} /></th>
  </tr>;
}

export default TargetsTableHeader;