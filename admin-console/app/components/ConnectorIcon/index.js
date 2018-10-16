import React from 'react';
import PropTypes from 'prop-types';

import ErrorIcon from '../../images/error-icon.png';
import UnknownIcon from '../../images/unknown-icon.png';
import KeyIcon from '../../images/key-icon.png';
import ReplicationKeyIcon from '../../images/replication-key-icon.png'

import MysqlLogo from '../../images/mysql-logo.png';
import PostgresLogo from '../../images/postgresql-logo.png';
import ZendeskLogo from '../../images/zendesk-logo.png';
import KafkaLogo from '../../images/kafka-logo.png';


function decoratorsByName(name) {
  if (name) {
    if (name === "key") {
      return { logo: KeyIcon, name: 'Key' };
    } else if (name === "replication-key") {
      return { logo: ReplicationKeyIcon, name: 'Replication Key' };
    } else if (name.match(/(tap|target)-mysql/)) {
      return { logo: MysqlLogo, name: 'MySQL' };
    } else if (name.match(/(tap|target)-postgres/)) {
      return { logo: PostgresLogo, name: 'PostgreSQL' };
    } else if (name.match(/(tap|target)-zendesk/)) {
      return { logo: ZendeskLogo, name: 'Zendesk' };
    } else if (name.match(/(tap|target)-kafka/)) {
      return { logo: KafkaLogo, name: 'Kafka' };
    } else {
      return { logo: UnknownIcon, name }
    }
  } else {
    return { logo: UnknownIcon, name: 'Not defined' };
  }
}

function ConnectorIcon(props) {
  const decorator = decoratorsByName(props.name);
  return <img className={props.className || "img-icon"} src={decorator.logo} />;
}

ConnectorIcon.propTypes = {
  name: PropTypes.any,
  className: PropTypes.any,
}

export default ConnectorIcon;