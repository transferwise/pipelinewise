import React from 'react';
import { Helmet } from 'react-helmet';

import Targets from '../Targets/Loadable';
import Target from '../Target/Loadable';
import Taps from '../Taps/Loadable';

/* eslint-disable react/prefer-stateless-function */
class ConnectionsPage extends React.Component {
  render() {
    const { match: { params } } = this.props;
    return (
      <main role="main" className="container">
        <Helmet>
          <title>Connections</title>
        </Helmet>
        <Targets selectedTargetId={params.target} />
        <hr className="full-width" />
        <Target targetId={params.target} />
        <hr className="full-width" />
        <Taps targetId={params.target} />
      </main>
    )
  }
}

export default ConnectionsPage;
