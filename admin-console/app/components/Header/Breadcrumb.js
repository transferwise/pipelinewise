import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

function Breadcrumb(location) {
  const pathItems = location.pathname && location.pathname.split('/');
  const breadcrumbItems = []

  // Find the targetId and tapId from the location URL
  const ids = pathItems.filter((p, i) => i === 2 || i === 4)

  // Add items to breadcrumbs array
  if (ids.length > 0) {
    if (ids[0]) {
      breadcrumbItems.push({ id: ids[0], name: ids[0], href: `/targets/${ids[0]}`, className: ids.length === 1 ? 'active' : '' });
    }
    if (ids[1]) {
      breadcrumbItems.push({ id: ids[1], name: ids[1], href: `/targets/${ids[0]}/taps/${ids[1]}`, className: ids.length === 2 ? 'active' : '' });
    }
  } else {
    breadcrumbItems.push({ id: 'Data Warehouses', name: messages.targets.defaultMessage, href: '/targets' });
  }

  return (
    <ul className="navbar-nav mr-auto">
      {breadcrumbItems.map((b, i) => (
          <li key={`item-${b.id}`} className={b.className}>
            <a className={`nav-link ${b.className}`} href={b.href}>{b.name}{i < (breadcrumbItems.length -1 )  ? `\u00a0\u00a0\u00a0/` : ''}</a>
          </li>
        )
      )}
    </ul>
  );
}

Breadcrumb.propTypes = {
  location: PropTypes.object,
};

export default Breadcrumb;