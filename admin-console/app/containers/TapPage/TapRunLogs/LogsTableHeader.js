import React from 'react';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function LogsTableHeader() {
  return <tr>
    <th />
    <th><FormattedMessage {...messages.createdAt} /></th>
    <th className="text-center"><FormattedMessage {...messages.status} /></th>
  </tr>;
}

export default LogsTableHeader;