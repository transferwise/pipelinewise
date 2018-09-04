import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { Grid, Alert, Row, Col } from 'react-bootstrap/lib';
import { Tab, Tabs, TabList, TabPanel } from 'react-tabs';
import 'react-tabs/style/react-tabs.css';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';

import ConnectorIcon from 'components/ConnectorIcon';
import messages from './messages';

function capitalise(str) {
  return str.charAt(0).toUpperCase() + str.substring(1);
}

function rTabList(tabs, props={ disabled: false, className: "react-tabs__tab", selectedClassName: "react-tabs__tab--selected" }) {
  return tabs.map((tab, i) => (
    <Tab
      key={`tab-${i}`}
      className={props.className}
      selectedClassName={props.selectedClassName}
      disabled={props.disabled}>
        {tab}
    </Tab>
  ));
}

function rTabPanel(tabs) {

  return tabs.map((t, i) => {
    const tab = t.tab;
    const tabPanel = t.tabPanel;
    const codeString = JSON.stringify(t.tabPanel, null, 4);

    if (codeString === 'null') {
      return (
        <TabPanel key={`tabpanel-${i}`}>
          <Alert bsStyle="warning"><strong>Error!</strong> <FormattedMessage {...messages.codeNotFound} /></Alert>
        </TabPanel>
      );
    }

    if (tab === 'Streams') {
      const streamTabs = tabPanel.map(stream => ({ tab: stream.stream, tabPanel: stream }));
      return (
        <TabPanel key={`tabpanel-${i}`}>
          <Tabs>
            <Grid>
              <Row>
                <Col md={3}>
                  <TabList>
                    {rTabList(
                      streamTabs.map(t => t.tab), {
                        className: "react-tabs__tab--vertical font-sm",
                        selectedClassName: "react-tabs__tab--selected-vertical"
                      })}
                  </TabList>
                </Col>
                <Col md={9}>
                  {rTabPanel(streamTabs)}
                </Col>
              </Row>
            </Grid>
          </Tabs>
        </TabPanel>
      );
    } 


    return (
      <TabPanel key={`tabpanel-${i}`}>
        <SyntaxHighlighter
          className="font-sm"
          language='json'
          style={light}
          showLineNumbers={true}>
            {codeString}
        </SyntaxHighlighter>
      </TabPanel>
    );
  });
}

function itemLocationString(location, itemName) {
  const flowName = location && (location.parentFlow || location.name);
  const target = location && location.target;

  return (
    <Grid>
      <Row>
        <Col md={6}>{flowName ? <span> <strong>Flow:</strong> {flowName}</span> : <span />}</Col>
        <Col md={6}>{(target === 'tap' || target === 'target') ? <span> <strong>{capitalise(target)}:</strong> <ConnectorIcon name={itemName} /></span> : <span />}</Col>
      </Row>
    </Grid>
  );
}

function FlowDetailsViewer({ selectedFlowItem }) {
  const location = selectedFlowItem.location;
  const value = selectedFlowItem.value;
  const itemName = value && value.name;
  const itemPath = value && value.path;
  const itemConfig = value && value.config;
  const itemProperties = value && value.properties;
  const itemState = value && value.state;
  const tabs = [];

  if (!selectedFlowItem) {
    return <Alert><strong>Tip!</strong> <FormattedMessage {...messages.flowItemNotSelected} /></Alert>
  }

  switch (location.target) {
    case 'flow': tabs.push(
      { tab: 'Basic', tabPanel: { name: itemName, path: itemPath }},
      { tab: 'Extraction Log', tabPanel: { }}
    ); break;
    case 'tap': tabs.push(
      { tab: 'Basic', tabPanel: { name: itemName, path: itemPath }},
      { tab: 'Config', tabPanel: itemConfig },
      { tab: 'Properties', tabPanel: itemProperties },
      { tab: 'State', tabPanel: itemState },
      { tab: 'Streams', tabPanel: itemProperties.streams }
    ); break;
    case 'target': tabs.push(
      { tab: 'Basic', tabPanel: { name: itemName, path: itemPath }},
      { tab: 'Config', tabPanel: itemConfig },
      { tab: 'Properties', tabPanel: itemProperties },
      { tab: 'State', tabPanel: itemState }
    ); break;
  }

  return (
    <div>
      {itemLocationString(location, itemName)}
      <Tabs defaultIndex={0}>
        <TabList>
          {rTabList(tabs.map(t => t.tab))}
        </TabList>
        {rTabPanel(tabs)}
      </Tabs>
    </div>
  );
}

FlowDetailsViewer.propTypes = {
  selectedFlowItem: PropTypes.any,
}

export default FlowDetailsViewer;
