/**
 * NotFoundPage
 *
 * This is the page we show when the user visits a url that doesn't have a route
 */

import React from 'react';
import { Helmet } from 'react-helmet';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

export default function NotFound() {
  return (
    <main role="main" className="container">
      <Helmet>
        <title>Not Found</title>
        <meta
          name="description"
          content="AnalyticsDB page not found"
        />
      </Helmet>
      <h1>
        <FormattedMessage {...messages.header} />
      </h1>
      <FormattedMessage {...messages.description} />
    </main>
  );
}
