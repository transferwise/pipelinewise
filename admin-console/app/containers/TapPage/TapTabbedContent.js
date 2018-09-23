import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { Grid, Row, Col, Alert, Button } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import TabbedContent from 'components/TabbedContent';
import ConnectorIcon from 'components/ConnectorIcon';

import TapDangerZone from './TapDangerZone/Loadable';
import SingerTapConfig from '../../singerConnectors/SingerTapConfig';
import SingerTapProperties from '../../singerConnectors/SingerTapProperties';

import messages from './messages';

function valueToString(value) {
  return JSON.stringify(value, null, 4);
}

function codeContent(codeString) {
  return (
    <SyntaxHighlighter
      className="font-sm"
      language='json'
      style={light}
      showLineNumbers={true}>
        {codeString || '"<EMPTY>"'}
    </SyntaxHighlighter>
  )
}

function summaryContent(tap) {
  return (
    <Grid>
      <Row>
        <Col md={6}>
          <SingerTapConfig tap={tap} />
        </Col>
        <Col md={6}>
          <Grid className="shadow-sm p-3 mb-5 rounded">
            <h4>{messages.tapSummary.defaultMessage}</h4>
            <Row>
              <Col md={6}><strong><FormattedMessage {...messages.tapId} />:</strong></Col><Col md={6}>{tap.id}</Col>
              <Col md={6}><strong><FormattedMessage {...messages.tapName} />:</strong></Col><Col md={6}>{tap.name}</Col>
              <Col md={6}><strong><FormattedMessage {...messages.tapType} />:</strong></Col><Col md={6}><ConnectorIcon name={tap.type} /> {tap.type}</Col>
            </Row>
          </Grid>
          <TapDangerZone targetId={tap.target.id} tapId={tap.id} />
        </Col>
      </Row>
    </Grid>
  )
}

function propertiesContent(tap) {
  return <SingerTapProperties tap={tap} />
}

function stateContent(tap) {
  try { return codeContent(valueToString(tap.files.state)) }
  catch(e) {
    return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> State file content not found</Alert>
  }
}

function logContent(tap) {
  try { return codeContent(valueToString(tap.files.log)) }
  catch(e) {
    return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Log file content not found</Alert>
  }
}

function TapTabbedContent({ targetId, tap }) {
  const tabs = [
    { title: messages.summary.defaultMessage, content: summaryContent(tap) },
    { title: messages.properties.defaultMessage, content: propertiesContent(tap) },
    { title: messages.log.defaultMessage, content: logContent(tap) },
    { title: messages.state.defaultMessage, content: stateContent(tap) },
  ];

  return (
    <Grid>
      <TabbedContent tabs={tabs} />
    </Grid>
  );
}

TapTabbedContent.propTypes = {
  tabs: PropTypes.any,
}

export default TapTabbedContent;