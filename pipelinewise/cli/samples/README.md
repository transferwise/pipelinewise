PipelineWise Sample Project
---------------------------


This directory contains [PipelineWise](https://github.com/transferwise/pipelinewise) YAML config templates
for creating data pipelines from various sources to various destinations.


# How to use?

To enable YAML files rename the ones that you need. You will need to enable at least one `tap_....yml` and
one `target_...yml` file:

1. Enable the source (tap) and target files that you need by renaming at least one `tap_....yml` and one `target_...yml` file and removing the `.sample` postfixes. For example if you want to replicate data from MySQL to Snowflake:

```
  $ mv tap_mysql_mariadb.yml.sample tap_my_mysql_db_one.yml
  $ mv target_snowflake.yml.sample  target_snowflake.yml
```

2. Edit the the new files with your faviourite text editor

3. Import into pipelinewise
   
```
  $ pipelinewise import --dir . [--secret path-to-fault-secret-file]
```

4. Check if the configuration imported successfully:
```
  $ pipelinewise status
  Warehouse ID    Source ID     Enabled    Type       Status    Last Sync    Last Sync Result
  --------------  ------------  ---------  ---------  --------  -----------  ------------------
  snowflake       mysql_sample  True       tap-mysql  ready                  unknown
  1 pipeline(s)
```
