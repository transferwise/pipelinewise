import React from 'react';
import PropTypes from 'prop-types';
import { FormattedMessage } from 'react-intl';

import { TimeSeries, TimeRangeEvent, TimeRange } from "pondjs";
import { Charts, ChartContainer, ChartRow, EventChart, Resizable } from "react-timeseries-charts";
import { Alert, Grid, Col, Row } from 'react-bootstrap/lib';
import LoadingIndicator from 'components/LoadingIndicator';

import { formatDate, statusToObj } from 'utils/helper';


/* eslint-disable react/prefer-stateless-function */
class Timeline extends React.Component {
  constructor(props) {
    super(props)
    this.state = {}
  }

  componentDidMount() {
    const { logs } = this.props
    const series = Timeline.initSeries(logs)

    this.setState({
      series,
      timerange: series.timerange(),
    })
  }

  static statusToStyle(event, state) {
    const statusObj = event.get("statusObj")

    switch (state) {
      case "normal":
        return {
            fill: statusObj && statusObj.color
        };
      case "hover":
        return {
            fill: statusObj && statusObj.color,
            opacity: 0.4
        };
      case "selected":
        return {
            fill: statusObj && statusObj.color
        };
      default:
      //pass
    }
  }

  static initSeries(logs) {
    const minDate = new Date(new Date().getTime() - 7 *  86400 * 1000)
    logs = logs.filter(l => new Date(l.timestamp) > minDate)
    logs.sort((a, b) => a.timestamp < b.timestamp ? -1 : 1)

    const events = logs.map(
      ({ timestamp, status }) => {
        const startDate = new Date(timestamp)
        const endDate = new Date(startDate.getTime() + 300000) // Default length is 5 minutes
        const statusObj = statusToObj(status)
        const title = `${statusObj.message}: ${formatDate(timestamp)}`

        return new TimeRangeEvent(new TimeRange(startDate, endDate), { title, statusObj })
      }
    )

    const series = new TimeSeries({ name: "events", events })
    return series
  }

  onTimeRangeChanged(timerange) {
    this.setState({ timerange })
  }

  render() {
    const { loading, error } = this.props

    if (loading) {
      return <LoadingIndicator />;
    }

    if (error != false) {
      return <Alert bsStyle="danger"><strong>Error!</strong> {error.toString()}</Alert>
    }

    return (
      <Grid>
        <Row>
          <Col md={12}>
            { this.state.series && this.state.timerange
            ? <Resizable>
                <ChartContainer
                  timeRange={this.state.timerange}
                  enablePanZoom={true}
                  onTimeRangeChanged={(timerange) => this.onTimeRangeChanged(timerange)}
                >
                  <ChartRow height="30">
                    <Charts>
                        <EventChart
                            series={this.state.series}
                            size={45}
                            style={Timeline.statusToStyle}
                            label={e => e.get("title")}
                        />
                    </Charts>
                  </ChartRow>
                </ChartContainer>
              </Resizable>
            : <Grid />}
          </Col>
        </Row>
      </Grid>
    );
  }
}

Timeline.propTypes = {
  loading: PropTypes.bool,
  error: PropTypes.any,
  logs: PropTypes.any,
}

export default Timeline;
