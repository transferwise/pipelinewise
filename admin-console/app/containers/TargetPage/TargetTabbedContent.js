import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { Grid, Row, Col, Alert } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import TabbedContent from 'components/TabbedContent';
import ConnectorIcon from 'components/ConnectorIcon';

import TargetDangerZone from './TargetDangerZone/Loadable';
import TargetPostgresConfig from './TargetPostgres/LoadableConfig';
import Taps from '../Taps/Loadable';
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

function summaryContent(target) {
  return (
    <Grid>
      <Row>
        <Col md={6}>
          {configContent(target)}
        </Col>
        <Col md={6}>
          <Grid className="shadow-sm p-3 mb-5 rounded">
            <h4>{messages.targetSummary.defaultMessage}</h4>
            <Row>
              <Col md={6}><strong><FormattedMessage {...messages.targetId} />:</strong></Col><Col md={6}>{target.id}</Col>
              <Col md={6}><strong><FormattedMessage {...messages.targetName} />:</strong></Col><Col md={6}>{target.name}</Col>
              <Col md={6}><strong><FormattedMessage {...messages.targetType} />:</strong></Col><Col md={6}><ConnectorIcon name={target.type} /> {target.type}</Col>
            </Row>
          </Grid>
          <TargetDangerZone targetId={target.id} />
        </Col>
      </Row>
    </Grid>
  )
}

function configContent(target) {
  // Try to find tap specific layout
  switch (target.type) {
    case 'target-postgres': return <TargetPostgresConfig targetId={target.id} title={`${target.name} Connection Details`}/>
  }

  // Render standard tap config layout only with the raw JSON
  try {
    return codeContent(valueToString(target.files.config))
  }
  catch(e) {
    return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Config file not exist</Alert>
  }
}

function tapsContent(target) {
  return (
    <Grid>
      <Taps targetId={target.id} />
    </Grid>
  )
}

function TargetTabbedContent({ target }) {
  const tabs = [
    { title: messages.summary.defaultMessage, content: summaryContent(target) },
    { title: messages.taps.defaultMessage, content: tapsContent(target) },
  ];

  return (
    <Grid>
      <TabbedContent tabs={tabs} />
    </Grid>
  );
}

TargetTabbedContent.propTypes = {
  tabs: PropTypes.any,
}

export default TargetTabbedContent;