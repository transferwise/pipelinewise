
import React from 'react';
import { compose } from 'redux';

import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';

export class SingerComponent extends React.PureComponent {
  valueToString(value) {
    return JSON.stringify(value, null, 4);
  }

  codeContent(codeString) {
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

  renderJson(json) {
    // Render config as ras JSON
    try {
      return this.codeContent(this.valueToString(json))
    }
    catch(e) {
      return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Config file not exist</Alert>
    }
  }
}

SingerComponent.propTypes = {}

export default compose()(SingerComponent);