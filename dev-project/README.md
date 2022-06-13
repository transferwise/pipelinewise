# Sample Project for Docker Development Environment

This is a sample project that is compatible with the Docker Development Environment
provided by this repository.

The local development environment comes with the following containers and components:
* PipelineWise CLI with every supported tap and target connectors
* MariaDB test source database with test data (for tap-mysql)
* Postgres test source database with test data (for tap-postgres)
* Postgres test target data warehouse (for target-postgres)
* Test Project that replicates data from MariaDB and Postgres databases into a Postgres Data Warehouse
* Integration and End to End test cases

## How to use

Install [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

Go to the main folder of the repository (the parent of this one) and To create local development environment:

```sh
$ cd dev-project/
$ docker-compose up --build
```

Wait until `PipelineWise Dev environment is ready in Docker container(s).` message. At the first run this can
run up to 5-10 minutes depending on your computer and your network connection. Once it's completed every
container, virtual environment and environment variables are set configured.

Open another terminal and shell into the PipelineWise container:

```sh
$ docker exec -it pipelinewise_dev bash
```

Import the dev project:

```sh
$ pipelinewise import --dir /opt/pipelinewise/dev-project/pipelinewise-config
```

Check the status, you should see multiple pipelines. Each of them is replicating data from different taps to Postgres DWH.
Every source database is filled with some test data.

```sh
$ pipelinewise status

Tap ID    Tap Type      Target ID     Target Type      Enabled    Status    Last Sync    Last Sync Result
--------  ------------  ------------  ---------------  ---------  --------  -----------  ------------------
postgres  tap-postgres  postgres_dwh  target-postgres  True       ready                  unknown
mariadb   tap-mysql     postgres_dwh  target-postgres  True       ready                  unknown
2 pipeline(s)
```

**Note**: To configure the list of tables to replicate, replication methods, load time transformations, etc.,
edit the YAML files in the `dev-project` directory. Don't forget to re-import the project with
`pipelinewise import --dir dev-project` when making changes in the YAML files.

To start replicating data from source MariaDB to target Postgres DWH:

```sh
$ pipelinewise run_tap --tap mariadb_db --target postgres_dwh
```

**Note**: Log files are generated at each run at `~/.pipelinewise/postgres_dwh/mariadb_db/log/`.
State file with incremental and log based positions generated at `~/.pipelinewise/postgres_dwh/mariadb_db/state.json`.
Next time when running the same command, the incrementally and log based (CDC) replicated tables
will capture the changes starting from the previously replicated position.

To start replicating data from source Postgres to target Postgres DWH:

```sh
$ pipelinewise run_tap --tap postgres_db --target postgres_dwh
```

**Note**: Log files are generated at each run at `~/.pipelinewise/postgres_dwh/postgres_db/log/`
State file with incremental and log based positions generated at `~/.pipelinewise/postgres_dwh/postgres_db/state.json`.
Next time when running the same command, the incrementally and log based (CDC) replicated tables
will capture the changes starting from the previously replicated position.

If you want to connect to any of the test databases by a db client (CLI, MySQL Workbench, pgAdmin, intelliJ, DataGrip, etc.),
check the [dev-project/.env](../dev-project/.env) file for the credentials.

###  Running tests

To run tests:

First, create a .env file from the .env.template file. On your local machine run:

```sh
cd dev-project
cp .env.template .env
```

Then, from within the container:

```sh
$ cd /opt/pipelinewise
$ pytest
```

To run tests and report code coverage:

```
$ coverage run -m pytest && coverage report
```

To generate HTML coverage report.

```
$ coverage run -m pytest && coverage html -d coverage_html
```

**Note**: The HTML report will be generated in `coverage_html/index.html`
and can be opened **only** from the docker host and not inside from the container.

###  Configuring end to end tests

You can customise which end to end tests you want to run by editing
[dev-project/.env](../dev-project/.env) file. By default only the open source taps and targets are selected because only these databases can run in docker containers for free. However end to end test cases are available for commercial databases and data stores as well including S3, Snowflake, Redshift.

To enable taps and targets to non open source data stores, add valid credentials to [dev-project/.env](../dev-project/.env) and the related tests cases will run automatically.

### To refresh the containers

To refresh the containers with new local code changes stop the running instances with `ctrl+c` and restart as usual:

```sh
$ docker-compose up --build
```

