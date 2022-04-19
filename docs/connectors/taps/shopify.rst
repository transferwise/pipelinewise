
.. _tap-shopify:

Tap Shopify
-----------

Configure access to your Shopify store
''''''''''''''''''''''''''''''''''''''

In order to extract your Shopify data, you will need:

- Store Subdomain
- Private App API Password
- Start Date

Store Subdomain
'''''''''''''''

The store subdomain can be derived from your Shopify admin URL.

If your admin URL starts with :code:`https://my-first-store.myshopify.com/`, your store subdomain is
:code:`my-first-store`.


Private App API Password
''''''''''''''''''''''''

You need to create a `Private App <https://help.shopify.com/en/manual/apps/private-apps>`_
API password to extract data from your Shopify Shop:

1. Log in to your Shopify store admin at :code:`https://<store subdomain>.myshopify.com/admin`
2. Click "Apps" in the sidebar on the left
3. On the bottom of the page, click "Manage private apps" next to "Working with a developer on your shop?"
4. Click the "Create a new private app" button
5. Enter a "Private app name" of your choosing, e.g. "Singer"
6. Enter your email address under "Emergency developer email"
7. In the "Admin API" section, click "▼ Review disabled Admin API permissions"
8. Choose "Read access" rather than "No access" in the access level dropdowns for the following permissions:

   1. Products, variants and collections - :code:`read_products, write_products`
   2. Orders, transactions and fulfillments - :code:`read_orders, write_orders`
   3. Customer details and customer groups - :code:`read_customers, write_customers`

9. Click "Save"
10. In the modal that appears, click "I understand, create the app"

Once the app has been created, you can locate the API password:

1. In the "Admin API" section on the private app details page, find the "Password" field and click "Show"
2. The value that appears (starting with :code:`shppa_`) is your API password.

Start Date
''''''''''

This property determines how much historical data will be extracted.


Configuring what to extract
'''''''''''''''''''''''''''

PipelineWise configures every tap with a common structured YAML file format.
A sample YAML for Jira replication can be generated into a project directory by
following the steps in the :ref:`generating_pipelines` section.

Example YAML for ``tap-shopify``:

.. code-block:: yaml

    ---

    # ------------------------------------------------------------------------------
    # General Properties
    # ------------------------------------------------------------------------------
    id: "shopify"                           # Unique identifier of the tap
    name: "Shopify"                         # Name of the tap
    type: "tap-shopify"                     # !! THIS SHOULD NOT CHANGE !!
    owner: "somebody@foo.com"               # Data owner to contact
    #send_alert: False                      # Optional: Disable all configured alerts on this tap
    #slack_alert_channel: "#tap-channel"   # Optional: Sending a copy of specific tap alerts to this slack channel


    # ------------------------------------------------------------------------------
    # Source (Tap) - Shopify connection details
    # ------------------------------------------------------------------------------
    db_conn:
      shop: "<STORE SUBDOMAIN>"               # Shopify Store Subdomain
      api_key: "<PRIVATE API KEY PASSWORD>"   # Shopify Private App API Password
      start_date: "2019-01-01"                # Sync data from this date onwards


    # ------------------------------------------------------------------------------
    # Destination (Target) - Target properties
    # Connection details should be in the relevant target YAML file
    # ------------------------------------------------------------------------------
    target: "snowflake"                       # ID of the target connector where the data will be loaded
    batch_size_rows: 20000                    # Batch size for the stream to optimise load performance
    stream_buffer_size: 0                     # In-memory buffer size (MB) between taps and targets for asynchronous data pipes
    default_target_schema: "shopify"          # Target schema where the data will be loaded
    #batch_wait_limit_seconds: 3600           # Optional: Maximum time to wait for `batch_size_rows`. Available only for snowflake target.

    # Options only for Snowflake target
    #archive_load_files: False                      # Optional: when enabled, the files loaded to Snowflake will also be stored in `archive_load_files_s3_bucket`
    #archive_load_files_s3_prefix: "archive"        # Optional: When `archive_load_files` is enabled, the archived files will be placed in the archive S3 bucket under this prefix.
    #archive_load_files_s3_bucket: "<BUCKET_NAME>"  # Optional: When `archive_load_files` is enabled, the archived files will be placed in this bucket. (Default: the value of `s3_bucket` in target snowflake YAML)


    # ------------------------------------------------------------------------------
    # Source to target Schema mapping
    # ------------------------------------------------------------------------------
    schemas:

      - source_schema: "shopify"             # This is mandatory, but can be anything in this tap type
        target_schema: "shopify"             # Target schema in the destination Data Warehouse
        #target_schema_select_permissions:    # Optional: Grant SELECT on schema and tables that created
        #  - grp_stats

        # List of Github tables to load into destination Data Warehouse
        # Tap-Github will use the best incremental strategies automatically to replicate data
        tables:
          # Supported tables
          - table_name: "orders"
          - table_name: "customers"
          - table_name: "products"
          - table_name: "transactions"


          # Additional supported tables
          #- table_name: "custom_collections"
          #- table_name: "abandoned_checkouts"
          #- table_name: "metafields"
          #- table_name: "order_refunds"
          #- table_name: "collects"


            # OPTIONAL: Load time transformations - you can add it to any table
            #transformations:
            #  - column: "some_column_to_transform" # Column to transform
            #    type: "SET-NULL"                   # Transformation type
