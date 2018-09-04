import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import messages from './messages';


function createLink({ href, message, location }) {
  let className = 'nav-item';

  if (href === location.pathname) {
    className = 'nav-item active';
  }

  return ({ href, message, className });
}

function HeaderLinksList({ location }) {
  const links = [
    createLink({ href: '/targets', message: messages.integrations, location }),
    createLink({ href: '/destinations', message: messages.target, location }),
  ];

  const content = links.map(link => (
    <li key={`item-${link.href}`} className={link.className}>
      <a className="nav-link" href={link.href}><FormattedMessage {...link.message} /> <span className="sr-only">(current)</span></a>
    </li>));

  return (
    <ul className="navbar-nav mr-auto">
      {content}
    </ul>
  );
}

HeaderLinksList.propTypes = {
  location: PropTypes.object,
};

export default HeaderLinksList;