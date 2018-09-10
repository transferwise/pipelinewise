import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import LocaleToggle from 'containers/LocaleToggle';
import HeaderLinksList from './HeaderLinksList';
import messages from './messages';

import Logo from '../../images/hummingbird.png';

/* eslint-disable react/prefer-stateless-function */
class Header extends React.Component {
  render() {
    const { location } = this.props;
    const HeaderLinksListProps = { location };

    return (     
      <header>
        <nav className="navbar navbar-expand-md navbar-dark fixed-top bg-blue">
          <a className="navbar-brand" href="/">
            <img className="img-icon-md" src={Logo}/>
            &nbsp;<FormattedMessage {...messages.brand} />  
          </a>
          <button className="navbar-toggler" type="button" data-toggle="collapse" data-target="#navbarCollapse" aria-controls="navbarCollapse" aria-expanded="false" aria-label="Toggle navigation">
            <span className="navbar-toggler-icon"></span>
          </button>
          <div className="collapse navbar-collapse" id="navbarCollapse">
            <HeaderLinksList {...HeaderLinksListProps} />
          </div>
        </nav>
      </header>    
    );
  }
}

export default Header;
