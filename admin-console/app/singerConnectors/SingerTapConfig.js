
import React from 'react';
import PropTypes from 'prop-types';
import { compose } from 'redux';

import SingerComponent from './SingerComponent';
import TapPostgresConfig from './TapPostgres/LoadableConfig';


export class SingerTapConfig extends SingerComponent {
  render() {
    const { tap } = this.props;
    
    if (tap) {
      // Try to find tap specific layout
      switch (tap.type) {
        case 'tap-postgres': return <TapPostgresConfig targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>

        default: return this.renderJson(tap.config)
      }
    }
  }
}

SingerTapConfig.propTypes = {
  tap: PropTypes.any,
}

export default compose()(SingerTapConfig);