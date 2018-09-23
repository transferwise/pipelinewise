import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { Grid, Row, Col, Alert } from 'react-bootstrap/lib';
import TabbedContent from 'components/TabbedContent';
import ConnectorIcon from 'components/ConnectorIcon';

import TargetDangerZone from './TargetDangerZone/Loadable';
import SingerTargetConfig from '../../singerConnectors/SingerTargetConfig';
import Taps from '../Taps/Loadable';
import messages from './messages';

function summaryContent(target) {
  return (
    <Grid>
      <Row>
        <Col md={6}>
          <SingerTargetConfig target={target} />
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