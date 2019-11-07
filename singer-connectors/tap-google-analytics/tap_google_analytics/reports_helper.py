import singer
import sys

from tap_google_analytics.ga_client import GAClient

LOGGER = singer.get_logger()

class ReportsHelper:
    def __init__(self, config, reports_definition):
        self.reports_definition = reports_definition

        # Fetch the valid (dimension, metric) names and their types from GAClient
        self.client = GAClient(config)

    def generate_catalog(self):
        """
        Generate the catalog based on the reports definition

        Assumptions:
        + All streams and attributes are automatically included
        + All dimensions are also defined as keys
        + There is a custom metadata keyword defined for all schema attributes:
          ga_type: dimension | metric
          This keyword is required for processing the catalog afterwards and
           generating the query to be send to GA API, as dimensions and metrics
           are not treated equally (they are sent as separate lists of attributes)
        + The {start_date, end_date} params for the report query are also added
           to the schema.
          This is important for defining the date range the records are for,
           especially when 'ga:date' is not part of the requested Dimensions.

          If 'ga:date' has not been added as one of the Dimensions, then the
           {start_date, end_date} attributes are also added as keys.

          For example, if a user requests to see user stats by device or by source,
           the {start_date, end_date} can be used as part of the key uniquelly
           identifying the generated stats.

          That way we can properly identify and update rows over overlapping
           runs of the tap.

        + The available (dimensions, metrics) and their data type are dynamically
          fetched using the GAClient.

          We use those lists to validate the dimension or metric names requested

          We also use those lists to set the data type for those attributes and
          cast the values accordingly (in case of integer or numeric values)

        Returns:
            A valid Singer.io Catalog.
        """
        streams = []

        for report in self.reports_definition:
            # For each report in reports_definition generate a Catalog Entry
            schema_name = report['name']

            schema = {
                "type": ["null", "object"],
                "additionalProperties": False,
                "properties": {}
            }

            metadata = []
            stream_metadata = {
                "metadata": {
                    "inclusion": "automatic",
                    "table-key-properties": None
                },
                "breadcrumb": []
            }
            table_key_properties = []

            # Track if there is a date set as one of the Dimensions
            date_dimension_included = False

            # Add the dimensions to the schema and as key_properties
            for dimension in report['dimensions']:
                if dimension == 'ga:date':
                    date_dimension_included = True

                data_type = self.client.lookup_data_type('dimension', dimension)
                dimension = dimension.replace("ga:","ga_")
                schema['properties'][dimension] = {
                    "type": [data_type],
                }

                table_key_properties.append(dimension)

                metadata.append({
                    "metadata": {
                        "inclusion": "automatic",
                        "selected-by-default": True,
                        "ga_type": 'dimension'
                    },
                    "breadcrumb": ["properties", dimension]
                })

            # Add the metrics to the schema
            for metric in report['metrics']:
                data_type = self.client.lookup_data_type('metric', metric)
                metric = metric.replace("ga:","ga_")

                schema['properties'][metric] = {
                    # metrics are allowed to also have null values
                    "type": ["null",data_type],
                }

                metadata.append({
                    "metadata": {
                        "inclusion": "automatic",
                        "selected-by-default": True,
                        "ga_type": 'metric'
                    },
                    "breadcrumb": ["properties", metric]
                })

            # Also add the {start_date, end_date} params for the report query
            schema['properties']['report_start_date'] = {
                "type": ["string"],
            }

            schema['properties']['report_end_date'] = {
                "type": ["string"],
            }

            # If 'ga:date' has not been added as a Dimension, add the
            #  {start_date, end_date} params as keys
            if not date_dimension_included:
                table_key_properties.append('report_start_date')
                table_key_properties.append('report_end_date')

            stream_metadata['metadata']['table-key-properties'] = table_key_properties

            # Add the Stream metadata (empty breadcrumb) to the start of the
            #  metada list so that everything is neatly organized in the Catalog
            metadata.insert(0, stream_metadata)

            # create and add catalog entry
            catalog_entry = {
                'stream': schema_name,
                'tap_stream_id': schema_name,
                'schema': schema,
                'metadata' : metadata
            }
            streams.append(catalog_entry)

        return {'streams': streams}

    def validate(self):
        for report in self.reports_definition:
            try:
                name = report['name']
                dimensions = report['dimensions']
                metrics = report['metrics']
            except KeyError:
                LOGGER.critical("Report definition is missing one of the required properties (name, dimensions, metrics)")
                sys.exit(1)

            # Check that not too many metrics && dimensions have been requested
            if len(metrics) == 0:
                LOGGER.critical("'{}' has no metrics defined. GA reports must specify at least one metric.".format(name))
                sys.exit(1)
            elif len(metrics) > 10:
                LOGGER.critical("'{}' has too many metrics defined. GA reports can have maximum 10 metrics.".format(name))
                sys.exit(1)

            if len(dimensions) > 7:
                LOGGER.critical("'{}' has too many dimensions defined. GA reports can have maximum 7 dimensions.".format(name))
                sys.exit(1)

            self.validate_dimensions(dimensions)
            self.validate_metrics(metrics)

            # ToDo: We should also check that the given metrics can be used
            #  with the given dimensions
            # Not all dimensions and metrics can be queried together. Only certain
            #  dimensions and metrics can be used together to create valid combinations.

    def validate_dimensions(self, dimensions):
        # check that all the dimensions are proper Google Analytics Dimensions
        for dimension in dimensions:
            if not dimension.startswith(('ga:dimension', 'ga:customVarName', 'ga:customVarValue')) \
               and dimension not in self.client.dimensions_ref:
                LOGGER.critical("'{}' is not a valid Google Analytics dimension".format(dimension))
                LOGGER.info("For details see https://developers.google.com/analytics/devguides/reporting/core/dimsmets")
                sys.exit(1)

    def validate_metrics(self, metrics):
        # check that all the metrics are proper Google Analytics metrics
        for metric in metrics:
            if metric.startswith('ga:goal') and metric.endswith(('Starts', 'Completions', 'Value', 'ConversionRate', 'Abandons', 'AbandonRate')):
                # Custom Google Analytics Metrics {ga:goalXXStarts, ga:goalXXValue, ... }
                continue
            elif metric.startswith('ga:searchGoal') and metric.endswith('ConversionRate'):
                # Custom Google Analytics Metrics ga:searchGoalXXConversionRate
                continue
            elif not metric.startswith(('ga:metric', 'ga:calcMetric')) \
               and metric not in self.client.metrics_ref:
                LOGGER.critical("'{}' is not a valid Google Analytics metric".format(metric))
                LOGGER.info("For details see https://developers.google.com/analytics/devguides/reporting/core/dimsmets")
                sys.exit(1)

    @staticmethod
    def get_report_definition(stream):
        report = {
            "name" : stream['tap_stream_id'],
            "dimensions" : [],
            "metrics" : []
        }

        stream_metadata = singer.metadata.to_map(stream['metadata'])

        for attribute in stream['schema']['properties'].keys():
            ga_type = singer.metadata.get(stream_metadata, ('properties', attribute), "ga_type")

            if ga_type == 'dimension':
                report['dimensions'].append(attribute)
            elif ga_type == 'metric':
                report['metrics'].append(attribute)

        return report
