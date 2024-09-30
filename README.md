# xdmod-htcss
XDMoD module that supports data from HTCondor Software Suite (HTCSS)

## Instructions

### Install module:

```
$ dnf install /path/to/rpm
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p ingest-organizations  -p ingest-resource-types -p xdmod.ingest-resources -a xdmod.staging-ingest-common.resource-specs -p xdmod.hpcdb-ingest-common -p xdmod.hpcdb-xdw-ingest-common
```

### Ingestion and Aggregation commands:

```
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p htcss.htcss-ingest -d 'HTCSS_LOG_DIR=/root/data/htcss/raw_aggregate/daily_logs/'
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p ingest-organizations  -p ingest-resource-types -p xdmod.ingest-resources -a xdmod.staging-ingest-common.resource-specs -p xdmod.hpcdb-ingest-common -p xdmod.hpcdb-xdw-ingest-common
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p htcss.htcss-aggregate --last-modified-start-date "$start_date"
$ acl-config
$ xdmod-build-filter-lists -r Jobs
```

The first command ingests logs into the database. It will ingest any file that matches the pattern of `YYYY-MM-DDTH:mm:ss_YYYY-MM-DDTHH:mm:ss.json`. The only files in this folder should be the ones you want to ingest when the command is run.

For the `--last-modified-start-date flag`, the time provided should be before `htcss-ingest` pipeline was run.

### Hierarchy

The hierarchy is a three level hierarchy with the following levels (bottom level first): Project, Field of Science, Program. The Project and Field Of Science information comes from the log files. The row for each entry has three fields: name, label, and it's parent. For the name and label, I used the project or field of science. The value for parent is the name of its parent element in the hierarchy. For a Project, this will be its Field of Science. As new projects or fields of science you will want to re-ingest this file in full.

More information can be found here, https://open.xdmod.org/10.5/hierarchy

```
$ xdmod-import-csv -t hierarchy -i /path/to/file/hierarchy.csv
```

### Names

The names are for both PI's and User's. The format is specified here, https://open.xdmod.org/10.5/user-names.html. Since the log files just provide a name for the PI's, your file will just have PI names in it. If in the future you get names for the users they can be added to the file. As new PI's are added you will want to re-ingest this file in full.

```
$ xdmod-import-csv -t names -i /path/to/file/names.csv
```

After ingesting the hierarchy or names files, the following commands should be run:

```
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p ingest-organizations  -p ingest-resource-types -p xdmod.ingest-resources -a xdmod.staging-ingest-common.resource-specs -p xdmod.hpcdb-ingest-common -p xdmod.hpcdb-xdw-ingest-common
$ php /usr/share/xdmod/tools/etl/etl_overseer.php -p htcss.htcss-aggregate --last-modified-start-date "$start_date"
```
