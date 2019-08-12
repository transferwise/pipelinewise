# Test Project for Docker

This is a sample test project that is compatible with the Development Environment
in docker containers provided by this repository.

The local development environment comes with the following containers and components:
* PipelineWise CLI with every supported tap and target connectors
* MariaDB test source database with test data (for tap-mysql)
* Postgres test source database with test data (for tap-postgres)
* Postgres test target data warehouse (for target-postgres)
* Test Project that replicates data from MariaDB and Postgres databases into a Postgres Data Warehouse

## How to use

Install [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/).

Go to the main folder of the repository (the parent of this one) and To create local development environment:
```
$ docker-compose up --build
```
Wait until `PipelineWise Dev environment is ready in Docker container(s).` message. At the first run this can
run up to 5-10 minutes depending on your computer and your network connection. Once it's completed every
container, virtual environment and environment variables are set configured.

Open another terminal and shell into the PipelineWise container:

```sh
$ docker exec -it pipelinewise_dev bash
```

Import the test project:

```sh
$ pipelinewise import_config --dir test-project
```

Check the status, you should see two pipelines. One is replicating data from MariaDB to Postgres DWH and
another replicating from Postgres to the same Postgres DWH:

```sh
$ pipelinewise status

Tap ID    Tap Type      Target ID     Target Type      Enabled    Status    Last Sync    Last Sync Result
--------  ------------  ------------  ---------------  ---------  --------  -----------  ------------------
postgres  tap-postgres  postgres_dwh  target-postgres  True       ready                  unknown
mariadb   tap-mysql     postgres_dwh  target-postgres  True       ready                  unknown
2 pipeline(s)
```

**Note**: To configure the list of tables to replicate, replication methods, load time transformations, etc.,
edit the YAML files in the `test-project` directory. Don't forget to re-import the project when making changes
in the YAML files.

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
check the `.env` file in the main folder of the repository for the credentials.

