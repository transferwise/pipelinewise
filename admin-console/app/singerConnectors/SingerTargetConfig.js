
import React from 'react';
import PropTypes from 'prop-types';
import { compose } from 'redux';

import SingerComponent from './SingerComponent';
import TargetPostgresConfig from './TargetPostgres/LoadableConfig';
import TargetSnowflakeConfig from './TargetSnowflake/LoadableConfig';


export class SingerTargetConfig extends SingerComponent {
  render() {
    const { target } = this.props;
    
    if (target) {
      // Try to find tap specific layout
      switch (target.type) {
        case 'target-postgres': return <TargetPostgresConfig targetId={target.id} title={`${target.name} Connection Details`}/>
        case 'target-snowflake': return <TargetSnowflakeConfig targetId={target.id} title={`${target.name} Connection Details`}/>

        default: return this.renderJson(target.files.config)
      }
    }
  }
}

SingerTargetConfig.propTypes = {
  target: PropTypes.any,
}

export default compose()(SingerTargetConfig);