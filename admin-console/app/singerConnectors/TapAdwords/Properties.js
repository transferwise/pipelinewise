import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';
import Modal from 'components/Modal';
import TransformationDropdown from 'components/TransformationDropdown';
import ReplicationMethodDropdown from 'components/ReplicationMethodDropdown';
import {
  makeSelectStreams,
  makeSelectForceRefreshStreams,
  makeSelectActiveStream,
  makeSelectActiveStreamId,
  makeSelectLoading,
  makeSelectError,
  makeSelectConsoleOutput
} from './selectors';
import {
  loadStreams,
  setActiveStreamId,
  discoverTap,
  updateStreams,
  updateStream,
  setTransformation,
  resetConsoleOutput
} from './actions';
import reducer from './reducer';
import saga from './saga';

import Toggle from 'react-toggle';
import { FormattedMessage } from 'react-intl';

import { Grid, Alert, Row, Col, ButtonGroup, Button } from 'react-bootstrap/lib';
import SyntaxHighlighter from 'react-syntax-highlighter/prism';
import { light } from 'react-syntax-highlighter/styles/prism';

import Table from 'components/Table';
import messages from './messages';


/* eslint-disable react/prefer-stateless-function */
export class TapZendeskProperties extends React.PureComponent {
  static streamsTableHeader(props) {
    const targetId = props.delegatedProps.targetId;
    const tapId = props.delegatedProps.tapId;
    const allStreamsSelected = props.delegatedProps.allStreamsSelected
    return (
      <tr>
        <th>
          <Toggle
            key={`stream-toggle-all`}
            defaultChecked={allStreamsSelected}
            onChange={() => props.delegatedProps.onUpdateStreams(
              targetId,
              tapId,
              {
                tapType: "tap-adwords",
                breadcrumb: [],
                update: {
                  key: "selected",
                  value: !allStreamsSelected
                }
              })}
          />
        </th>
        <th><FormattedMessage {...messages.table} /></th>
        <th><FormattedMessage {...messages.replicationMethod} /></th>
        <th></th>
      </tr>
    )
  }

  static streamsTableBody(props) {
    const item = props.item;
    const tableBreadcrumb = item.metadata.find(m => m.breadcrumb.length === 0);
    const tableMetadata = tableBreadcrumb.metadata || {};
    const replicationMethod = tableMetadata['forced-replication-method'];
    const targetId = props.delegatedProps.targetId;
    const tapId = props.delegatedProps.tapId;
    const stream = item["stream"];
    const streamId = item['tap_stream_id'];
    const isNew = item['is-new'];
    const isModified = item['is-modified'];
    const isSelected = props.selectedItem === streamId;
    let streamChangeDescription;

    if (isNew) {
      streamChangeDescription = <FormattedMessage {...messages.newTable} />
    } else if (isModified) {
      streamChangeDescription = <FormattedMessage {...messages.modifiedTable} />
    }

    return (
      <tr className={`${isSelected ? "table-active" : ""} ${streamChangeDescription ? "table-warning" : ""}`} onClick={() => props.onItemSelect(stream, streamId)}>
        <td>
          <Toggle
            key={`stream-toggle-${streamId}`}
            defaultChecked={tableMetadata.selected}
            onChange={() => props.delegatedProps.onUpdateStream(
              targetId,
              tapId,
              streamId,
              {
                tapType: "tap-adwords",
                breadcrumb: [],
                update: {
                  key: "selected",
                  value: !tableMetadata.selected
                }
              })}
          />
        </td>
        <td>{item['stream']}</td>
        <td>
          <ReplicationMethodDropdown
            value={replicationMethod}
            disabled={true}
            onChange={() => {}}
          />
        </td>
        <td>{item['is-new'] ? <FormattedMessage {...messages.newTable} /> : ''}</td>
      </tr>
    )
  }

  static columnsTableHeader() {
    return (
      <tr>
        <th></th>
        <th><FormattedMessage {...messages.column} /></th>
        <th><FormattedMessage {...messages.type} /></th>
        <th><FormattedMessage {...messages.replicationMethod} /></th>
        <th><FormattedMessage {...messages.transformation} /></th>
        <th></th>
      </tr>
    )
  }

  static columnsTableBody(props) {
    const item = props.item;
    const targetId = props.delegatedProps.targetId;
    const tapId = props.delegatedProps.tapId;
    const stream = props.delegatedProps.stream;
    const isAutomatic = item.inclusion === 'automatic';
    const selectedByDefault = item.selectedByDefault;
    const isSelected = (item.selected === undefined ? selectedByDefault : item.selected) || isAutomatic || item.isPrimaryKey;
    const isUnsupported = item.inclusion === 'unsupported';
    let method = <FormattedMessage {...messages.notSelected} />
    const transformationType = item.transformationType;
    let schemaChangeDescription;

    if (isAutomatic) {
      method = <FormattedMessage {...messages.automatic} />
    } else if (isSelected) {
      method = <FormattedMessage {...messages.selected} />
    }

    if (item.isNew) {
      schemaChangeDescription = <FormattedMessage {...messages.newColumn} />
    } else if (item.isModified) {
      schemaChangeDescription = <FormattedMessage {...messages.modifiedColumn} />
    }

    return (
      <tr className={schemaChangeDescription ? "table-warning" : ""}>
        <td>
          <Toggle
            key={`column-toggle-${item.name}`}
            defaultChecked={true}
            disabled={true}
            onChange={() => {}}
          />
        </td>
        <td>
          {item.isPrimaryKey && <ConnectorIcon className="img-icon-sm" name="key" />}&nbsp;
          {item.isReplicationKey && <ConnectorIcon className="img-icon-sm" name="replication-key" />}&nbsp;
          {item.name}
        </td>
        <td>{item.type}</td>
        <td>{method}</td>
        <td>
          <TransformationDropdown
            value={transformationType}
            disabled={item.isPrimaryKey || item.isReplicationKey || isUnsupported }
            onChange={(value) => props.delegatedProps.onTransformationChange(targetId, tapId, stream, item.name, value)}
          />
        </td>
        <td>{schemaChangeDescription}</td>
      </tr>
    )
  }

  static getColumnsFromStream(stream, props) {
    const schema = stream.schema.properties;
    const transformations = stream.transformations;

    return Object.keys(schema).map(col => {
      const mdataCols = stream.metadata.filter(m => m.breadcrumb.length > 0)
      const mdata = mdataCols.find(m => m.breadcrumb[1] === col).metadata
      const transformation = transformations.find(t => t['stream'] == props.activeStream && t['fieldId'] == col) || {}
      return {
        name: col,
        format: schema[col].format,
        type: Array.isArray(schema[col].type) ? (schema[col].type[1] || schema[col].type[0]) : schema[col].description,
        isPrimaryKey: Array.isArray(props.tableKeys) && props.tableKeys.indexOf(col) > -1,
        isReplicationKey: col === props.replicationKey,
        inclusion: schema[col].inclusion,
        selectedByDefault: mdata['selected-by-default'],
        selected: mdata['selected'],
        isNew: mdata['is-new'],
        isModified: mdata['is-modified'],
        transformationType: transformation.type || 'STRAIGHT_COPY',
      }})
  }

  componentDidMount() {
    const { targetId, tapId } = this.props
    this.props.onLoadStreams(targetId, tapId);
  }

  componentDidUpdate(prevProps) {
    const { targetId, tapId, forceRefreshStreams } = this.props
    if (forceRefreshStreams) {
      this.props.onLoadStreams(targetId, tapId);
    }
  }

  renderReplicationKeyDropdown() {
    const { streams, activeStream, activeStreamId, targetId, tapId, onUpdateStream } = this.props
    let stream

    if (activeStreamId) {
      stream = streams.find(s => s['tap_stream_id'] === activeStreamId);

      if (stream) {
        const mdataStream = stream.metadata.find(m => m.breadcrumb.length === 0).metadata || {}
        const replicationMethod = mdataStream['forced-replication-method']
        const replicationKeys = mdataStream['valid-replication-keys']
        const tableKeys = mdataStream['table-key-properties']
        const columns = TapZendeskProperties.getColumnsFromStream(stream, { activeStream, replicationKeys, tableKeys })
        const availableKeyColumns = columns.filter(c => c.type === 'integer' || c.format === 'date-time')

        // Show key dropdown only for Key-Based incremental replication
        if (replicationMethod === 'INCREMENTAL') {
          return (
            <Grid className="text-right form-group row">
              <label htmlFor="replication-key-dropdown" className="col-md-6 col-form-label">{messages.replicationKey.defaultMessage}:</label>
              <Col md={6}>
                <select
                  id="replication-key-dropdown"
                  className="form-control"
                  value={(Array.isArray.replicationKeys && replicationKeys[0]) || (Array.isArray.tableKeys && Array.isArray.tableKeys[0])}
                  disabled={true}
                  onChange={() => {}}
                >
                  {(!replicationKeys ? [<option key={`col-not-defined`} className="text-danger" value="NOT_DEFINED">{messages.replicationKeyNotDefined.defaultMessage}</option>] : [])
                    .concat(availableKeyColumns.map(c => <option key={`col-${c.name}`} value={c.name}>{c.name} ({c.type.toUpperCase()})</option>))}
                </select>
              </Col>
            </Grid>
          )
        }
      }
    }

    return <Grid />
  }

  renderStreamColumns(streams, activeStream, activeStreamId, delegatedProps) {
    if (activeStreamId) {
      const stream = streams.find(s => s['tap_stream_id'] === activeStreamId);

      if (stream) {
        const mdataStream = stream.metadata.find(m => m.breadcrumb.length === 0).metadata || {}
        const replicationKey = mdataStream['replication-key']
        const tableKeys = mdataStream['table-key-properties']
        const columns = TapZendeskProperties.getColumnsFromStream(stream, { activeStream, replicationKey, tableKeys })
        return (
          <Grid>
            <Table
              items={columns}
              headerComponent={TapZendeskProperties.columnsTableHeader}
              bodyComponent={TapZendeskProperties.columnsTableBody}
              delegatedProps={delegatedProps}
            />
          </Grid>
        )
      } else {
        return <Alert bsStyle="info" className="full-swidth"><FormattedMessage {...messages.streamNotSelected} /></Alert>
      }
    } else {
      return <Alert bsStyle="info" className="full-swidth"><FormattedMessage {...messages.streamNotSelected} /></Alert>
    }
  }

  render() {
    const {
      loading,
      error,
      consoleOutput,
      targetId,
      tapId,
      streams,
      activeStream,
      activeStreamId,
      onStreamSelect,
      onUpdateStreams,
      onUpdateStream,
      onTransformationChange,
      onDiscoverTap,
      onCloseModal
    } = this.props;
    let alert = <div />;
    let consolePanel = <div />

    if (loading) {
      return <LoadingIndicator />;
    }

    try {
      if (error !== false) {
        alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>
      }

      if (consoleOutput !== false) {
        consolePanel =
          <SyntaxHighlighter className="font-sssm" language='shsssell' style={light}
            showLineNumbers={false}>
              {consoleOutput}
          </SyntaxHighlighter>
      }

      // Find selected tables
      let selectedStreams = []
      if (streams) {
        selectedStreams = streams.filter(s => {
          const tableBreadcrumb = s.metadata.find(m => m.breadcrumb.length === 0)
          const tableMetadata = tableBreadcrumb.metadata || {}
          return tableMetadata.selected
        })
      }

      // Used for select/deselect all
      const allStreamsSelected = Array.isArray(streams) ? selectedStreams.length === streams.length : false;

      // Find selected columns
      let columns
      let selectedColumns = []
      if (streams) {
        const stream = streams.find(s => s['tap_stream_id'] === activeStreamId);
        columns = stream ? TapZendeskProperties.getColumnsFromStream(stream, {}) : []
        selectedColumns = columns.filter(c => (c.selected === undefined ? c.selectedByDefault : c.selected) || c.isAutomatic)
      }

      return (
        <Grid>
          <h5>{messages.properties.defaultMessage}</h5>
          {consoleOutput ?
            <Modal
              show={consoleOutput}
              title={<FormattedMessage {...messages.discoverErrorTitle} />}
              body={<Grid>{alert}{consolePanel}</Grid>}
              onClose={() => onCloseModal()} />
          : alert }

          <Row>
            <Col md={12}>
              <ButtonGroup bsClass="float-right">
                <Button bsStyle="primary" onClick={() => onDiscoverTap(targetId, tapId)}><FormattedMessage {...messages.discover} /></Button>
              </ButtonGroup>
            </Col>
          </Row>

          {Array.isArray(streams) ?
            <Grid>
              <Row>
                <Col md={12}>
                  <strong><FormattedMessage {...messages.tablesToReplicate} /></strong>&nbsp;
                  {streams ? `(${selectedStreams.length} / ${streams.length} selected)` : ''}
                  <Grid className="table-wrapper-scroll-y">
                    <Table
                      items={streams}
                      selectedItem={activeStreamId}
                      headerComponent={TapZendeskProperties.streamsTableHeader}
                      bodyComponent={TapZendeskProperties.streamsTableBody}
                      onItemSelect={onStreamSelect}
                      delegatedProps={{ targetId, tapId, onUpdateStreams, onUpdateStream, allStreamsSelected }}
                    />
                  </Grid>
                </Col>
              </Row>
              <hr className="full-width" />
          <Row>
          </Row>
              <Row>
                <Col md={5}>
                  <strong><FormattedMessage {...messages.columnsToReplicate} /></strong>&nbsp;
                  {columns ? `(${selectedColumns.length} / ${columns.length} selected)` : ''}
                  <br />
                </Col>
                <Col md={7}>
                  {this.renderReplicationKeyDropdown()}
                </Col>
              </Row>
              <Row>
                <Col md={12}>
                  <Grid>
                    {this.renderStreamColumns(streams, activeStream, activeStreamId, { targetId, tapId, stream: activeStream, streamId: activeStreamId, onUpdateStream, onTransformationChange })}
                  </Grid>
                </Col>
              </Row>
            </Grid>
          : <Grid />}
        </Grid>
      )
    }
    catch(e) {
      return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Doesn't look like valid Zendesk tap properties: {e.toString()}</Alert>
    }
  }
}

TapZendeskProperties.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  targetId: PropTypes.any,
  tapId: PropTypes.any,
  streams: PropTypes.any,
  forceRefreshStreams: PropTypes.any,
  activeStreamId: PropTypes.any,
  onStreamSelect: PropTypes.func,
  onUpdateStreams: PropTypes.func,
  onUpdateStream: PropTypes.func,
  onTransformationChange: PropTypes.func,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadStreams: (targetId, tapId) => dispatch(loadStreams(targetId, tapId)),
    onStreamSelect: (stream, streamId) => dispatch(setActiveStreamId(stream, streamId)),
    onUpdateStreams: (targetId, tapId, params) => dispatch(updateStreams(targetId, tapId, params)),
    onUpdateStream: (targetId, tapId, streamId, params) => dispatch(updateStream(targetId, tapId, streamId, params)),
    onTransformationChange: (targetId, tapId, stream, fieldId, value) => dispatch(setTransformation(targetId, tapId, stream, fieldId, value)),
    onDiscoverTap: (targetId, tapId) => dispatch(discoverTap(targetId, tapId)),
    onCloseModal: () => dispatch(resetConsoleOutput())
  };
}

const mapStateToProps = createStructuredSelector({
  streams: makeSelectStreams(),
  forceRefreshStreams: makeSelectForceRefreshStreams(),
  activeStream: makeSelectActiveStream(),
  activeStreamId: makeSelectActiveStreamId(),
  loading: makeSelectLoading(),
  error: makeSelectError(),
  consoleOutput: makeSelectConsoleOutput(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapZendesk', reducer });
const withSaga = injectSaga({ key: 'tapZendesk', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapZendeskProperties);
