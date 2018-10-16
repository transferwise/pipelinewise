
import React from 'react';
import PropTypes from 'prop-types';
import { compose } from 'redux';

import SingerComponent from './SingerComponent';
import TapPostgresConfig from './TapPostgres/LoadableConfig';
import TapMysqlConfig from './TapMysql/LoadableConfig';
import TapZendeskConfig from './TapZendesk/LoadableConfig';
import TapKafkaConfig from './TapKafka/LoadableConfig';


export class SingerTapConfig extends SingerComponent {
  render() {
    const { tap } = this.props;
    
    if (tap) {
      // Try to find tap specific layout
      switch (tap.type) {
        case 'tap-postgres': return <TapPostgresConfig targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>
        case 'tap-mysql': return <TapMysqlConfig targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>
        case 'tap-zendesk': return <TapZendeskConfig targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>
        case 'tap-kafka': return <TapKafkaConfig targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>

        default: return this.renderJson(tap.config)
      }
    }
  }
}

SingerTapConfig.propTypes = {
  tap: PropTypes.any,
}

export default compose()(SingerTapConfig);