import React from 'react';
import PropTypes from 'prop-types';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { createStructuredSelector } from 'reselect';
import injectReducer from 'utils/injectReducer';
import injectSaga from 'utils/injectSaga';
import LoadingIndicator from 'components/LoadingIndicator';
import ConnectorIcon from 'components/ConnectorIcon';
import {
  makeSelectStreams,
  makeSelectForceRefreshStreams,
  makeSelectActiveStreamId,
  makeSelectLoading,
  makeSelectError
} from './selectors';
import { loadStreams, setActiveStreamId, updateStreamToReplicate } from './actions';
import reducer from './reducer';
import saga from './saga';

import Toggle from 'react-toggle';
import { FormattedMessage } from 'react-intl';

import { Grid, Alert, Row, Col } from 'react-bootstrap/lib';

import Table from 'components/Table';
import messages from './messages';


/* eslint-disable react/prefer-stateless-function */
export class TapPostgresProperties extends React.PureComponent {
  static streamsTableHeader() {
    return (
      <tr>
        <th></th>
        <th><FormattedMessage {...messages.database} /></th>
        <th><FormattedMessage {...messages.schema} /></th>
        <th><FormattedMessage {...messages.table} /></th>
        <th><FormattedMessage {...messages.isView} /></th>
        <th><FormattedMessage {...messages.rowCount} /></th>
        <th><FormattedMessage {...messages.replicationMethod} /></th>
      </tr>
    )
  }
  
  static streamsTableBody(props) {
    const item = props.item;
    const tableBreadcrumb = item.metadata.find(m => m.breadcrumb.length === 0);
    const tableMetadata = tableBreadcrumb.metadata || {};
    const targetId = props.delegatedProps.targetId;
    const tapId = props.delegatedProps.tapId;
    const streamId = item['tap_stream_id'];
    const isSelected = props.selectedItem === streamId;

    return (
      <tr className={isSelected ? "table-active" : ""} onClick={() => props.onItemSelect(streamId)}>
        <td>
          <Toggle
            key={`stream-toggle-${streamId}`}
            defaultChecked={tableMetadata.selected}
            onChange={() => props.delegatedProps.onUpdateStreamToReplicate(
              targetId,
              tapId,
              streamId,
              {
                tapType: "tap-postgres",
                breadcrumb: [],
                update: {
                  key: "selected",
                  value: !tableMetadata.selected
                }
              })}
          />
        </td>
        <td>{tableMetadata['database-name']}</td>
        <td>{tableMetadata['schema-name']}</td>
        <td>{item['table_name']}</td>
        <td>{item['is-view'] ? 'Yes' : ''}</td>
        <td>{tableMetadata['row-count']}</td>
        <td>{tableMetadata['replication-method'] || <FormattedMessage {...messages.notSelected} />}</td>
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
      </tr>
    )
  }
  
  static columnsTableBody(props) {
    const item = props.item;
    const targetId = props.delegatedProps.targetId;
    const tapId = props.delegatedProps.tapId;
    const streamId = props.delegatedProps.streamId;
    const isAutomatic = item.inclusion === 'automatic';
    const isSelected = item.selected || isAutomatic;
    let method = <FormattedMessage {...messages.notSelected} />

    if (isAutomatic) {
      method = <FormattedMessage {...messages.automatic} />
    } else if (isSelected) {
      method = <FormattedMessage {...messages.selected} />
    }

    return (
      <tr>
        <td>
          <Toggle
            key={`column-toggle-${item.name}`}
            defaultChecked={isSelected}
            disabled={item.isPrimaryKey}
            onChange={() => props.delegatedProps.onUpdateStreamToReplicate(
              targetId,
              tapId,
              streamId,
              {
                tapType: "tap-postgres",
                breadcrumb: ["properties", item.name],
                update: {
                  key: "selected",
                  value: !isSelected
                }
              })}
          /></td>
        <td>{item.isPrimaryKey && <ConnectorIcon className="img-icon-sm" name="key" />} {item.name}</td>
        <td>{item.type}</td>
        <td>{method}</td>
      </tr>
    )
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

  renderStreamColumns(streams, activeStreamId, delegatedProps) {
    if (activeStreamId) {
      const activeStream = streams.find(s => s['tap_stream_id'] === activeStreamId);
      const schema = activeStream.schema.properties;
      const mdataCols = activeStream.metadata.filter(m => m.breadcrumb.length > 0)
      const columns = Object.keys(schema).map(col => {
        const mdata = mdataCols.find(m => m.breadcrumb[1] === col).metadata
        return {
          name: col,
          format: schema[col].format,
          type: schema[col].type[1] || schema[col].type[0],
          isPrimaryKey: schema[col].type[0] === 'null' ? false : true,
          inclusion: mdata.inclusion,
          selectedByDefault: mdata['selected-by-default'],
          selected: mdata['selected'],
          sqlDatatype: mdata['sql-datatype'],
        }})

      return (
        <Grid>
          <Table
            items={columns}
            headerComponent={TapPostgresProperties.columnsTableHeader}
            bodyComponent={TapPostgresProperties.columnsTableBody}
            delegatedProps={delegatedProps}
          />
        </Grid>
      )
    } else {
      return <Alert bsStyle="info" className="full-swidth"><FormattedMessage {...messages.streamNotSelected} /></Alert>
    }
  }

  render() {
    const { loading, error, targetId, tapId, streams, activeStreamId, onStreamSelect, onUpdateStreamToReplicate } = this.props;
    let alert = <div />;
    
    if (loading) {
      return <LoadingIndicator />;
    }

    try {
      if (error !== false) {
        alert = <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> {error.toString()}</Alert>
      }

      return (
        <Grid>
          {alert}
          {Array.isArray(streams) ?
            <Grid>
              <Row>
                <Col md={12}> 
                  <strong><FormattedMessage {...messages.tablesToReplicate} /></strong>
                  <Grid className="table-wrapper-scroll-y">
                    <Table
                      items={streams}
                      selectedItem={activeStreamId}
                      headerComponent={TapPostgresProperties.streamsTableHeader}
                      bodyComponent={TapPostgresProperties.streamsTableBody}
                      onItemSelect={onStreamSelect}
                      delegatedProps={{ targetId, tapId, onUpdateStreamToReplicate }}
                    />
                  </Grid>
                </Col>
              </Row>
              <hr className="full-width" />
              <Row>
                <Col md={12}>
                  <strong><FormattedMessage {...messages.columnsToReplicate} /></strong>
                  <Grid>
                    {this.renderStreamColumns(streams, activeStreamId, { targetId, tapId, streamId: activeStreamId, onUpdateStreamToReplicate })}
                  </Grid>
                </Col>
              </Row>
            </Grid>
          : <Grid />}
        </Grid>
      )
    }
    catch(e) {
      return <Alert bsStyle="danger" className="full-swidth"><strong>Error!</strong> Doesn't look like valid PostgreSQL tap properties: {e.toString()}</Alert>
    }
  }
}

TapPostgresProperties.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  targetId: PropTypes.any,
  tapId: PropTypes.any,
  streams: PropTypes.any,
  forceRefreshStreams: PropTypes.any,
  activeStreamId: PropTypes.any,
  onStreamSelect: PropTypes.func,
  onUpdateStreamToReplicate: PropTypes.func,
}

export function mapDispatchToProps(dispatch) {
  return {
    onLoadStreams: (targetId, tapId) => dispatch(loadStreams(targetId, tapId)),
    onStreamSelect: (streamId) => dispatch(setActiveStreamId(streamId)),
    onUpdateStreamToReplicate: (targetId, tapId, streamId, params) => dispatch(updateStreamToReplicate(targetId, tapId, streamId, params)),
  };
}

const mapStateToProps = createStructuredSelector({
  streams: makeSelectStreams(),
  forceRefreshStreams: makeSelectForceRefreshStreams(),
  activeStreamId: makeSelectActiveStreamId(),
  loading: makeSelectLoading(),
  error: makeSelectError(),
});

const withConnect = connect(
  mapStateToProps,
  mapDispatchToProps,
);

const withReducer = injectReducer({ key: 'tapPostgres', reducer });
const withSaga = injectSaga({ key: 'tapPostgres', saga });

export default compose(
  withReducer,
  withSaga,
  withConnect,
)(TapPostgresProperties);