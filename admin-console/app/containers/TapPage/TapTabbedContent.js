import React from 'react';
import PropTypes from 'prop-types';

import { Grid, Row, Col, Alert } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import TabbedContent from 'components/TabbedContent';

import TapControlCard from './TapControlCard/Loadable';
import TapInheritableConfig from './TapInheritableConfig';
import TapDangerZone from './TapDangerZone/Loadable';
import TapRunLogs from './TapRunLogs/Loadable';
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
          <TapDangerZone targetId={tap.target.id} tapId={tap.id} />
        </Col>
        <Col md={6}>
          <TapControlCard targetId={tap.target.id} tapId={tap.id} />
          <TapInheritableConfig targetId={tap.target.id} tapId={tap.id} />
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
  return <TapRunLogs targetId={tap.target.id} tapId={tap.id} />
}

function TapTabbedContent(props) {
  const { tap } = props

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

export default TapTabbedContent;