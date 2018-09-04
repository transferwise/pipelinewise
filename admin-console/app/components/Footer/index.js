import React from 'react';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function Footer() {
  return (
    <footer className="footer">
      <div className="container">
        <span className="text-muted"><FormattedMessage {...messages.license} /></span> -&nbsp;
        <span className="text-muted"><FormattedMessage {...messages.author} /></span>
      </div>
    </footer>
  );
}

export default Footer;
