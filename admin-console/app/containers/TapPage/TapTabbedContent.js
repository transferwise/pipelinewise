import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { Grid, Alert } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';
import TabbedContent from 'components/TabbedContent';

import TapPostgresConfig from './TapPostgres/Config';
import TapPostgresProperties from './TapPostgres/Loadable';
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
      kakamaci
    </Grid>
  )
}

function configContent(targetId, tap) {
  // Try to find tap specific layout
  switch (tap.type) {
    case 'tap-postgres': return <TapPostgresConfig tap={tap} config={tap.files.config} />
  }

  // Render standard tap config layout only with the raw JSON
  try {
    return codeContent(valueToString(tap.files.config))
  }
  catch(e) {
    return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Config file not exist</Alert>
  }
}

function propertiesContent(targetId, tap) {
  // Try to find tap specific layout
  switch (tap.type) {
    case 'tap-postgres': return <TapPostgresProperties targetId={targetId} tapId={tap.id} />
  }

  // Render standard tap properties layout only with the raw JSON
  try {
    return codeContent(valueToString(tap.files.properties))
  }
  catch(e) {
    return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Properties file content not found</Alert>
  }
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
    { title: messages.properties.defaultMessage, content: propertiesContent(targetId, tap) },
    { title: messages.log.defaultMessage, content: logContent(tap) },
    { title: messages.state.defaultMessage, content: stateContent(tap) },
    { title: messages.config.defaultMessage, content: configContent(targetId, tap) },
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