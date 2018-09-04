import React from 'react';
import PropTypes from 'prop-types';

import { Grid, Row, Col } from 'react-bootstrap/lib';
import { Tab, Tabs, TabList, TabPanel } from 'react-tabs';
import 'react-tabs/style/react-tabs.css';

function rTabList(titles, props={ disabled: false }) {
  const className = props.orientation === 'vertical' ? 'react-tabs__tab--vertical' : 'react-tabs__tab'; 
  const selectedClassName = props.orientation === 'vertical' ? 'react-tabs__tab--selected-vertical' : 'react-tabs__tab--selected';

  return titles.map((title, i) => (
    <Tab
      key={`tab-${i}`}
      className={className}
      selectedClassName={selectedClassName}
      disabled={props.disabled}>
        {title}
    </Tab>
  ));
}

function rTabPanel(contents) {
  return contents.map((content, i) => {
    return (
      <TabPanel key={`tabpanel-${i}`}>
        {content}
      </TabPanel>
    )
  });
}

function TabbedContent(props) {
  const { tabs, orientation } = props;
  const tabListProps = { orientation };

  if (orientation === 'vertical') {
    return (
      <Grid>
        <Tabs defaultIndex={0}>
          <Grid>
            <Row>
              <Col md={3}>
                <TabList>
                  {rTabList(tabs.map(t => t.title), tabListProps)}
                </TabList>
              </Col>
              <Col md={9}>
                {rTabPanel(tabs.map(t => t.content))}
              </Col>
            </Row>
          </Grid>
        </Tabs>
      </Grid>
    )  
  }

  return (
    <Grid>
      <Tabs defaultIndex={0}>
        <TabList>
          {rTabList(tabs.map(t => t.title), tabListProps)}
        </TabList>
        {rTabPanel(tabs.map(t => t.content))}
      </Tabs>
    </Grid>
  )
}

TabbedContent.propTypes = {
  tabs: PropTypes.array,
  orientation: PropTypes.any,
}

export default TabbedContent;