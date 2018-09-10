import React from 'react';
import { Helmet } from 'react-helmet';
import { FormattedMessage } from 'react-intl';

import messages from './messages';

/* eslint-disable react/prefer-stateless-function */
export default class HomePage extends React.Component {
  render() {
    return (
      <main role="main" className="container-fluid">
        <Helmet>
          <title>Home Page</title>
          <meta
            name="description"
            content="ETLWise home page"
          />
        </Helmet>
        <h1 className="mt-5"><FormattedMessage {...messages.mainTopic} /></h1>
        <p><FormattedMessage {...messages.intro} /></p>
      </main>
    );
  }
}