# Sample Project for Docker Development Environment

This is a sample project that is compatible with the Docker Development Environment
provided by this repository.

The local development environment comes with the following containers and components:
* PipelineWise CLI with every supported tap and target connectors
* MariaDB test source database with test data (for tap-mysql)
* MySQL 8 test source database with test data (for tap-mysql)
* Postgres test source database with test data (for tap-postgres)
* YugabyteDB test source database with test data (for tap-yugabyte)
* MongoDB replicaSet test source database with test data (for tap-mongodb)
* Postgres test target data warehouse (for target-postgres)
* Dedicated Postgres operational database for PipelineWise data-diff state
* Test Project that replicates data from MariaDB, Postgres, and MongoDB databases into a Postgres Data Warehouse
* Integration and End to End test cases

Two Postgres containers with separate databases, roles, host ports, Docker networks,
and volumes serve distinct purposes:

* `pipelinewise-postgres-target` — the replication target. Holds replicated table
  data only.
* `pipelinewise-backend-db` — **not** a replication target. Holds generic scheduler
  state, data-diff definitions, preflights, run evidence, and coverage watermarks,
  and nothing else.

## How to use

Install [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

From this directory, create the `.env` file and start the environment:

```sh
$ cd dev-project/
$ cp .env.template .env
$ docker compose up --build
```

Wait until `PipelineWise Dev environment is ready in Docker container(s).` message. At the first run this can
run up to 5-10 minutes depending on your computer and your network connection. Once it's completed every
container, virtual environment and environment variables are set configured.

Open another terminal and shell into the PipelineWise container:

```sh
$ docker exec -it pipelinewise bash
```

Import the dev project:

```sh
$ pipelinewise import_config --dir /opt/pipelinewise/dev-project/pipelinewise-config
```

Check the status, you should see multiple pipelines. Each of them is replicating data from different taps to Postgres DWH.
Every source database is filled with some test data.

```sh
$ pipelinewise status

Tap ID                  Tap Type      Target ID        Target Type      Enabled    Status    Last Sync    Last Sync Result
----------------------  ------------  ---------------  ---------------  ---------  --------  -----------  ------------------
tap_postgres            tap-postgres  target_postgres  target-postgres  True       ready                  unknown
tap_mariadb             tap-mysql     target_postgres  target-postgres  True       ready                  unknown
tap_mongodb_to_pg       tap-mongodb   target_postgres  target-postgres  True       ready                  unknown
3 pipeline(s)
```

**Note**: To configure the list of tables to replicate, replication methods, load time transformations, etc.,
edit the YAML files in `dev-project/pipelinewise-config`. Don't forget to re-run the
`import_config` command above after changing them.

### Replicating data

Run any of the taps against the Postgres DWH, for example:

```sh
$ pipelinewise run_tap --tap tap_mariadb --target target_postgres
$ pipelinewise run_tap --tap tap_postgres --target target_postgres
$ pipelinewise run_tap --tap tap_yugabyte --target target_postgres
$ pipelinewise run_tap --tap tap_mongodb_to_pg --target target_postgres
```

**Note**: Each run writes logs to `~/.pipelinewise/<target>/<tap>/log/` and a state
file to `~/.pipelinewise/<target>/<tap>/state.json`. The state file holds the
incremental and log based (CDC) positions, so the next run of the same command
captures changes starting from the previously replicated position.

### Data-diff checks

The LOG_BASED Postgres example in `tap_postgres_logical.yml` includes a
table-level data-diff definition for `logical1.logical1_table1`. To replicate the
table, inspect the persisted definition, and run one scheduler batch:

```sh
$ pipelinewise run_tap --tap tap_postgres_logical --target target_postgres
$ pipelinewise list_data_diff_checks \
    --tap tap_postgres_logical --target target_postgres
$ pipelinewise list_scheduled_jobs --job-type data_diff
$ pipelinewise run_scheduler \
    --job-type data_diff \
    --once \
    --tap tap_postgres_logical --target target_postgres
```

The check compares the source table with its table in
`pipelinewise-postgres-target`.

### Connecting with a database client

To connect to any of the test databases with a db client (CLI, MySQL Workbench, pgAdmin, intelliJ, DataGrip, etc.),
check the [dev-project/.env](../dev-project/.env) file for the credentials. The
target PostgreSQL service is exposed on `TARGET_POSTGRES_PORT_ON_HOST`; the
independent backend is exposed on `PIPELINEWISE_BACKEND_PORT_ON_HOST`.

###  Running tests

From within the container:

```sh
$ cd /opt/pipelinewise
$ pytest tests/
```

To report code coverage, or write an HTML report instead:

```sh
$ coverage run -m pytest tests/ && coverage report
$ coverage run -m pytest tests/ && coverage html -d coverage_html
```

**Note**: The HTML report will be generated in `coverage_html/index.html`
and can be opened **only** from the docker host and not inside from the container.

###  Configuring end to end tests

You can customise which end to end tests you want to run by editing
check the [dev-project/.env](../dev-project/.env) file. By default only the open source taps and targets are selected because only these databases can run in docker containers for free. However end to end test cases are available for commercial databases and data stores as well including S3 and Snowflake.

To enable taps and targets to non open source data stores, add valid credentials to [dev-project/.env](../dev-project/.env) and the related tests cases will run automatically.

### To refresh the containers

To refresh the containers with new local code changes stop the running instances with `ctrl+c` and restart as usual:

```sh
$ docker compose up --build
```
