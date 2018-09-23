/**
 *
 * App
 *
 * This component is the skeleton around the actual pages, and should only
 * contain code that should be seen on all pages. (e.g. navigation bar)
 */

import React from 'react';
import { Helmet } from 'react-helmet';
import styled from 'styled-components';
import { Switch, Route } from 'react-router-dom';

import Header from 'components/Header';
import TargetsPage from 'containers/TargetsPage/Loadable';
import TargetPage from 'containers/TargetPage/Loadable';
import TapPage from 'containers/TapPage/Loadable';
import NotFoundPage from 'containers/NotFoundPage/Loadable';
import Footer from 'components/Footer';

const AppWrapper = styled.div`
`;

export default function App() {
  return (
    <AppWrapper>
      <Helmet
        titleTemplate="%s - PipelineWise"
        defaultTitle="PipelineWise"
      >
        <meta name="description" content="PipelineWise" />
      </Helmet>
      <Route component={Header} />
      <Switch>
        <Route exact path="/" component={TargetsPage} />
        <Route exact path="/targets" component={TargetsPage} />
        <Route exact path="/targets/:target" component={TargetPage} />
        <Route exact path="/targets/:target/taps/:tap" component={TapPage} />
        <Route path="" component={NotFoundPage} />
      </Switch>
      <Footer />
    </AppWrapper>
  );
}
