
import React from 'react';
import PropTypes from 'prop-types';
import { compose } from 'redux';

import SingerComponent from './SingerComponent';
import TapPostgresProperties from './TapPostgres/LoadableProperties';
import TapMysqlProperties from './TapMysql/LoadableProperties';


export class SingerTapProperties extends SingerComponent {
  render() {
    const { tap } = this.props;
    
    if (tap) {
      // Try to find tap specific layout
      switch (tap.type) {
        case 'tap-postgres': return <TapPostgresProperties targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>
        case 'tap-mysql': return <TapMysqlProperties targetId={tap.target.id} tapId={tap.id} title={`${tap.name} Connection Details`}/>

        default: return this.renderJson(tap.properties)
      }
    
      // Render standard tap properties layout only with the raw JSON
      try {
        return this.codeContent(this.valueToString(target.files.properties))
      }
      catch(e) {
        return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Config file not exist</Alert>
      }
    }
  }
}

SingerTapProperties.propTypes = {
  tap: PropTypes.any,
}

export default compose()(SingerTapProperties);