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
import HomePage from 'containers/HomePage/Loadable';
import AddTargetPage from 'containers/AddTargetPage/Loadable';
import ConnectionsPage from 'containers/ConnectionsPage/Loadable';
import TapPage from 'containers/TapPage/Loadable';
import AddTapPage from 'containers/AddTapPage/Loadable';
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
        <Route exact path="/" component={HomePage} />
        <Route exact path="/add" component={AddTargetPage} />
        <Route exact path="/targets" component={ConnectionsPage} />
        <Route exact path="/targets/:target" component={ConnectionsPage} />
        <Route exact path="/targets/:target/add" component={AddTapPage} />
        <Route exact path="/targets/:target/taps/:tap" component={TapPage} />
        <Route path="" component={NotFoundPage} />
      </Switch>
      <Footer />
    </AppWrapper>
  );
}
