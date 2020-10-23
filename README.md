[![Build Status](https://travis-ci.org/postgrespro/pg_pathman.svg?branch=master)](https://travis-ci.org/postgrespro/pg_pathman)
[![PGXN version](https://badge.fury.io/pg/pg_pathman.svg)](https://badge.fury.io/pg/pg_pathman)
[![codecov](https://codecov.io/gh/postgrespro/pg_pathman/branch/master/graph/badge.svg)](https://codecov.io/gh/postgrespro/pg_pathman)
[![GitHub license](https://img.shields.io/badge/license-PostgreSQL-blue.svg)](https://raw.githubusercontent.com/postgrespro/pg_pathman/master/LICENSE)

### NOTE: this project is not under development anymore

`pg_pathman` supports Postgres versions [9.5..13], but most probably it won't be ported to 14 and later releases. [Native partitioning](https://www.postgresql.org/docs/current/ddl-partitioning.html) is pretty mature now and has almost everything implemented in `pg_pathman`'; we encourage users switching to it. We are still maintaining the project (fixing bugs in supported versions), but no new development is going to happen here.

# pg_pathman

The `pg_pathman` module provides optimized partitioning mechanism and functions to manage partitions.

The extension is compatible with:

 * PostgreSQL 9.5, 9.6, 10, 11, 12, 13;
 * Postgres Pro Standard 9.5, 9.6, 10, 11, 12;
 * Postgres Pro Enterprise;

Take a look at our Wiki [out there](https://github.com/postgrespro/pg_pathman/wiki).

## Overview
**Partitioning** means splitting one large table into smaller pieces. Each row in such table is moved to a single partition according to the partitioning key. PostgreSQL <= 10 supports partitioning via table inheritance: each partition must be created as a child table with CHECK CONSTRAINT:

```plpgsql
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

PostgreSQL 10 provides native partitioning:

```plpgsql
CREATE TABLE test(id int4, value text) PARTITION BY RANGE(id);
CREATE TABLE test_1 PARTITION OF test FOR VALUES FROM (1) TO (10);
CREATE TABLE test_2 PARTITION OF test FOR VALUES FROM (10) TO (20);
```

It's not so different from the classic approach; there are implicit check constraints, and most of its limitations are still relevant.

Despite the flexibility, this approach forces the planner to perform an exhaustive search and to check constraints on each partition to determine whether it should be present in the plan or not. Large amount of partitions may result in significant planning overhead.

The `pg_pathman` module features partition managing functions and optimized planning mechanism which utilizes knowledge of the partitions' structure. It stores partitioning configuration in the `pathman_config` table; each row contains a single entry for a partitioned table (relation name, partitioning column and its type). During the initialization stage the `pg_pathman` module caches some information about child partitions in the shared memory, which is used later for plan construction. Before a SELECT query is executed, `pg_pathman` traverses the condition tree in search of expressions like:

```
VARIABLE OP CONST
```
where `VARIABLE` is a partitioning key, `OP` is a comparison operator (supported operators are =, <, <=, >, >=), `CONST` is a scalar value. For example:

```plpgsql
WHERE id = 150
```

Based on the partitioning type and condition's operator, `pg_pathman` searches for the corresponding partitions and builds the plan. Currently `pg_pathman` supports two partitioning schemes:

* **RANGE** - maps rows to partitions using partitioning key ranges assigned to each partition. Optimization is achieved by using the binary search algorithm;
* **HASH** - maps rows to partitions using a generic hash function.

More interesting features are yet to come. Stay tuned!

## Feature highlights

 * HASH and RANGE partitioning schemes;
 * Partitioning by expression and composite key;
 * Both automatic and manual [partition management](#post-creation-partition-management);
 * Support for integer, floating point, date and other types, including domains;
 * Effective query planning for partitioned tables (JOINs, subselects etc);
 * `RuntimeAppend` & `RuntimeMergeAppend` custom plan nodes to pick partitions at runtime;
 * [`PartitionFilter`](#custom-plan-nodes): an efficient drop-in replacement for INSERT triggers;
 * [`PartitionRouter`](#custom-plan-nodes) and [`PartitionOverseer`](#custom-plan-nodes) for cross-partition UPDATE queries (instead of triggers);
 * Automatic partition creation for new INSERTed data (only for RANGE partitioning);
 * Improved `COPY FROM` statement that is able to insert rows directly into partitions;
 * [User-defined callbacks](#additional-parameters) for partition creation event handling;
 * Non-blocking [concurrent table partitioning](#data-migration);
 * FDW support (foreign partitions);
 * Various [GUC](#disabling-pg_pathman) toggles and configurable settings.
 * Partial support of [`declarative partitioning`](#declarative-partitioning) (from PostgreSQL 10).

## Installation guide
To install `pg_pathman`, execute this in the module's directory:

```shell
make install USE_PGXS=1
```

> **Important:** Don't forget to set the `PG_CONFIG` variable (`make PG_CONFIG=...`) in case you want to test `pg_pathman` on a non-default or custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

Modify the **`shared_preload_libraries`** parameter in `postgresql.conf` as following:

```
shared_preload_libraries = 'pg_pathman'
```

> **Important:** `pg_pathman` may cause conflicts with some other extensions that use the same hook functions. For example, `pg_pathman` uses `ProcessUtility_hook` to handle COPY queries for partitioned tables, which means it may interfere with `pg_stat_statements` from time to time. In this case, try listing libraries in certain order: `shared_preload_libraries = 'pg_stat_statements, pg_pathman'`.

It is essential to restart the PostgreSQL instance. After that, execute the following query in psql:
```plpgsql
CREATE EXTENSION pg_pathman;
```

Done! Now it's time to setup your partitioning schemes.

> **Windows-specific**: pg_pathman imports several symbols (e.g. None_Receiver, InvalidObjectAddress) from PostgreSQL, which is fine by itself, but requires that those symbols are marked as `PGDLLIMPORT`. Unfortunately, some of them are not exported from vanilla PostgreSQL, which means that you have to either use Postgres Pro Standard/Enterprise (which includes all necessary patches), or patch and build your own distribution of PostgreSQL.

## How to update
In order to update pg_pathman:

1. Install the latest _stable_ release of pg_pathman.
2. Restart your PostgreSQL cluster.
3. Execute the following queries:

```plpgsql
/* only required for major releases, e.g. 1.4 -> 1.5 */
ALTER EXTENSION pg_pathman UPDATE;
SET pg_pathman.enable = t;
```

## Available functions

### Module's version

```plpgsql
pathman_version()
```
Although it's possible to get major and minor version numbers using `\dx pg_pathman`, it doesn't show the actual [patch number](http://semver.org/). This function returns a complete version number of the loaded pg_pathman module in `MAJOR.MINOR.PATCH` format.

### Partition creation
```plpgsql
create_hash_partitions(parent_relid     REGCLASS,
                       expression       TEXT,
                       partitions_count INTEGER,
                       partition_data   BOOLEAN DEFAULT TRUE,
                       partition_names  TEXT[] DEFAULT NULL,
                       tablespaces      TEXT[] DEFAULT NULL)
```
Performs HASH partitioning for `relation` by partitioning expression `expr`. The `partitions_count` parameter specifies the number of partitions to create; it cannot be changed afterwards. If `partition_data` is `true` then all the data will be automatically copied from the parent table to partitions. Note that data migration may took a while to finish and the table will be locked until transaction commits. See `partition_table_concurrently()` for a lock-free way to migrate data. Partition creation callback is invoked for each partition if set beforehand (see `set_init_callback()`).

```plpgsql
create_range_partitions(parent_relid    REGCLASS,
                        expression      TEXT,
                        start_value     ANYELEMENT,
                        p_interval      ANYELEMENT,
                        p_count         INTEGER DEFAULT NULL
                        partition_data  BOOLEAN DEFAULT TRUE)

create_range_partitions(parent_relid    REGCLASS,
                        expression      TEXT,
                        start_value     ANYELEMENT,
                        p_interval      INTERVAL,
                        p_count         INTEGER DEFAULT NULL,
                        partition_data  BOOLEAN DEFAULT TRUE)

create_range_partitions(parent_relid    REGCLASS,
                        expression      TEXT,
                        bounds          ANYARRAY,
                        partition_names TEXT[] DEFAULT NULL,
                        tablespaces     TEXT[] DEFAULT NULL,
                        partition_data  BOOLEAN DEFAULT TRUE)
```
Performs RANGE partitioning for `relation` by partitioning expression `expr`, `start_value` argument specifies initial value, `p_interval` sets the default range for auto created partitions or partitions created with `append_range_partition()` or `prepend_range_partition()` (if `NULL` then auto partition creation feature won't work), `p_count` is the number of premade partitions (if not set then `pg_pathman` tries to determine it based on expression's values). The `bounds` array can be built using `generate_range_bounds()`. Partition creation callback is invoked for each partition if set beforehand.

```plpgsql
generate_range_bounds(p_start     ANYELEMENT,
                      p_interval  INTERVAL,
                      p_count     INTEGER)

generate_range_bounds(p_start     ANYELEMENT,
                      p_interval  ANYELEMENT,
                      p_count     INTEGER)
```
Builds `bounds` array for `create_range_partitions()`.


### Data migration

```plpgsql
partition_table_concurrently(relation   REGCLASS,
                             batch_size INTEGER DEFAULT 1000,
                             sleep_time FLOAT8 DEFAULT 1.0)
```
Starts a background worker to move data from parent table to partitions. The worker utilizes short transactions to copy small batches of data (up to 10K rows per transaction) and thus doesn't significantly interfere with user's activity. If the worker is unable to lock rows of a batch, it sleeps for `sleep_time` seconds before the next attempt and tries again up to 60 times, and quits if it's still unable to lock the batch.

```plpgsql
stop_concurrent_part_task(relation REGCLASS)
```
Stops a background worker performing a concurrent partitioning task. Note: worker will exit after it finishes relocating a current batch.

### Triggers

Triggers are no longer required nor for INSERTs, neither for cross-partition UPDATEs. However, user-supplied triggers *are supported*:

* Each **inserted row** results in execution of `BEFORE/AFTER INSERT` trigger functions of a *corresponding partition*.
* Each **updated row** results in execution of `BEFORE/AFTER UPDATE` trigger functions of a *corresponding partition*.
* Each **moved row** (cross-partition update) results in execution of `BEFORE UPDATE` + `BEFORE/AFTER DELETE` + `BEFORE/AFTER INSERT` trigger functions of *corresponding partitions*.

### Post-creation partition management
```plpgsql
replace_hash_partition(old_partition REGCLASS,
                       new_partition REGCLASS,
                       lock_parent   BOOLEAN DEFAULT TRUE)
```
Replaces specified partition of HASH-partitioned table with another table. The `lock_parent` parameter will prevent any INSERT/UPDATE/ALTER TABLE queries to parent table.


```plpgsql
split_range_partition(partition_relid REGCLASS,
                      split_value     ANYELEMENT,
                      partition_name  TEXT DEFAULT NULL,
                      tablespace      TEXT DEFAULT NULL)
```
Split RANGE `partition` in two by `split_value`. Partition creation callback is invoked for a new partition if available.

```plpgsql
merge_range_partitions(variadic partitions REGCLASS[])
```
Merge several adjacent RANGE partitions. Partitions are automatically ordered by increasing bounds; all the data will be accumulated in the first partition.

```plpgsql
append_range_partition(parent_relid   REGCLASS,
                       partition_name TEXT DEFAULT NULL,
                       tablespace     TEXT DEFAULT NULL)
```
Append new RANGE partition with `pathman_config.range_interval` as interval.

```plpgsql
prepend_range_partition(parent_relid   REGCLASS,
                        partition_name TEXT DEFAULT NULL,
                        tablespace     TEXT DEFAULT NULL)
```
Prepend new RANGE partition with `pathman_config.range_interval` as interval.

```plpgsql
add_range_partition(parent_relid   REGCLASS,
                    start_value    ANYELEMENT,
                    end_value      ANYELEMENT,
                    partition_name TEXT DEFAULT NULL,
                    tablespace     TEXT DEFAULT NULL)
```
Create new RANGE partition for `relation` with specified range bounds. If `start_value` or `end_value` are NULL then corresponding range bound will be infinite.

```plpgsql
drop_range_partition(partition TEXT, delete_data BOOLEAN DEFAULT TRUE)
```
Drop RANGE partition and all of its data if `delete_data` is true.

```plpgsql
attach_range_partition(parent_relid    REGCLASS,
                       partition_relid REGCLASS,
                       start_value     ANYELEMENT,
                       end_value       ANYELEMENT)
```
Attach partition to the existing RANGE-partitioned relation. The attached table must have exactly the same structure as the parent table, including the dropped columns. Partition creation callback is invoked if set (see `pathman_config_params`).

```plpgsql
detach_range_partition(partition_relid REGCLASS)
```
Detach partition from the existing RANGE-partitioned relation.

```plpgsql
disable_pathman_for(parent_relid REGCLASS)
```
Permanently disable `pg_pathman` partitioning mechanism for the specified parent table and remove the insert trigger if it exists. All partitions and data remain unchanged.

```plpgsql
drop_partitions(parent_relid REGCLASS,
                delete_data  BOOLEAN DEFAULT FALSE)
```
Drop partitions of the `parent` table (both foreign and local relations). If `delete_data` is `false`, the data is copied to the parent table first. Default is `false`.

To remove partitioned table along with all partitions fully, use conventional
`DROP TABLE relation CASCADE`. However, care should be taken in somewhat rare
case when you are running logical replication and `DROP` was executed by
replication apply worker, e.g. via trigger on replicated table. `pg_pathman`
uses `pathman_ddl_trigger` event trigger to remove the record about dropped
table from `pathman_config`, and this trigger by default won't fire on replica,
leading to inconsistent state when `pg_pathman` thinks that the table still
exists, but in fact it doesn't. If this is the case, configure this trigger to
fire on replica too:

```plpgsql
ALTER EVENT TRIGGER pathman_ddl_trigger ENABLE ALWAYS;
```

Physical replication doesn't have this problem since DDL as well as
`pathman_config` table is replicated too; master and slave PostgreSQL instances
are basically identical, and it is only harmful to keep this trigger in `ALWAYS`
mode.


### Additional parameters


```plpgsql
set_interval(relation REGCLASS, value ANYELEMENT)
```
Update RANGE partitioned table interval. Note that interval must not be negative and it must not be trivial, i.e. its value should be greater than zero for numeric types, at least 1 microsecond for `TIMESTAMP` and at least 1 day for `DATE`.

```plpgsql
set_enable_parent(relation REGCLASS, value BOOLEAN)
```
Include/exclude parent table into/from query plan. In original PostgreSQL planner parent table is always included into query plan even if it's empty which can lead to additional overhead. You can use `disable_parent()` if you are never going to use parent table as a storage. Default value depends on the `partition_data` parameter that was specified during initial partitioning in `create_range_partitions()` function. If the `partition_data` parameter was `true` then all data have already been migrated to partitions and parent table disabled. Otherwise it is enabled.

```plpgsql
set_auto(relation REGCLASS, value BOOLEAN)
```
Enable/disable auto partition propagation (only for RANGE partitioning). It is enabled by default.

```plpgsql
set_init_callback(relation REGCLASS, callback REGPROC DEFAULT 0)
```
Set partition creation callback to be invoked for each attached or created partition (both HASH and RANGE). If callback is marked with SECURITY INVOKER, it's executed with the privileges of the user that produced a statement which has led to creation of a new partition (e.g. `INSERT INTO partitioned_table VALUES (-5)`). The callback must have the following signature: `part_init_callback(args JSONB) RETURNS VOID`. Parameter `arg` consists of several fields whose presence depends on partitioning type:
```json
/* RANGE-partitioned table abc (child abc_4) */
{
    "parent":           "abc",
    "parent_schema":    "public",
    "parttype":         "2",
    "partition":        "abc_4",
    "partition_schema": "public",
    "range_max":        "401",
    "range_min":        "301"
}

/* HASH-partitioned table abc (child abc_0) */
{
    "parent":           "abc",
    "parent_schema":    "public",
    "parttype":         "1",
    "partition":        "abc_0",
    "partition_schema": "public"
}
```

```plpgsql
set_set_spawn_using_bgw(relation REGCLASS, value BOOLEAN)
```
When INSERTing new data beyond the partitioning range, use SpawnPartitionsWorker to create new partitions in a separate transaction.

## Views and tables

#### `pathman_config` --- main config storage
```plpgsql
CREATE TABLE IF NOT EXISTS pathman_config (
    partrel         REGCLASS NOT NULL PRIMARY KEY,
    expr            TEXT NOT NULL,
    parttype        INTEGER NOT NULL,
    range_interval  TEXT,
    cooked_expr     TEXT);
```
This table stores a list of partitioned tables.

#### `pathman_config_params` --- optional parameters
```plpgsql
CREATE TABLE IF NOT EXISTS pathman_config_params (
    partrel         REGCLASS NOT NULL PRIMARY KEY,
    enable_parent   BOOLEAN NOT NULL DEFAULT TRUE,
    auto            BOOLEAN NOT NULL DEFAULT TRUE,
    init_callback   TEXT DEFAULT NULL,
    spawn_using_bgw BOOLEAN NOT NULL DEFAULT FALSE);
```
This table stores optional parameters which override standard behavior.

#### `pathman_concurrent_part_tasks` --- currently running partitioning workers
```plpgsql
-- helper SRF function
CREATE OR REPLACE FUNCTION show_concurrent_part_tasks()
RETURNS TABLE (
    userid     REGROLE,
    pid        INT,
    dbid       OID,
    relid      REGCLASS,
    processed  INT,
    status     TEXT)
AS 'pg_pathman', 'show_concurrent_part_tasks_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW pathman_concurrent_part_tasks
AS SELECT * FROM show_concurrent_part_tasks();
```
This view lists all currently running concurrent partitioning tasks.

#### `pathman_partition_list` --- list of all existing partitions
```plpgsql
-- helper SRF function
CREATE OR REPLACE FUNCTION show_partition_list()
RETURNS TABLE (
    parent     REGCLASS,
    partition  REGCLASS,
    parttype   INT4,
    expr       TEXT,
    range_min  TEXT,
    range_max  TEXT)
AS 'pg_pathman', 'show_partition_list_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW pathman_partition_list
AS SELECT * FROM show_partition_list();
```
This view lists all existing partitions, as well as their parents and range boundaries (NULL for HASH partitions).

#### `pathman_cache_stats` --- per-backend memory consumption
```plpgsql
-- helper SRF function
CREATE OR REPLACE FUNCTION @extschema@.show_cache_stats()
RETURNS TABLE (
	context     TEXT,
	size        INT8,
	used        INT8,
	entries     INT8)
AS 'pg_pathman', 'show_cache_stats_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW @extschema@.pathman_cache_stats
AS SELECT * FROM @extschema@.show_cache_stats();
```
Shows memory consumption of various caches.

## Declarative partitioning

From PostgreSQL 10 `ATTACH PARTITION`, `DETACH PARTITION`
and `CREATE TABLE .. PARTITION OF` commands could be used with tables
partitioned by `pg_pathman`:

```plpgsql
CREATE TABLE child1 (LIKE partitioned_table);

--- attach new partition
ALTER TABLE partitioned_table ATTACH PARTITION child1
	FOR VALUES FROM ('2015-05-01') TO ('2015-06-01');

--- detach the partition
ALTER TABLE partitioned_table DETACH PARTITION child1;

-- create a partition
CREATE TABLE child2 PARTITION OF partitioned_table
	FOR VALUES IN ('2015-05-01', '2015-06-01');
```

## Custom plan nodes
`pg_pathman` provides a couple of [custom plan nodes](https://wiki.postgresql.org/wiki/CustomScanAPI) which aim to reduce execution time, namely:

- `RuntimeAppend` (overrides `Append` plan node)
- `RuntimeMergeAppend` (overrides `MergeAppend` plan node)
- `PartitionFilter` (drop-in replacement for INSERT triggers)
- `PartitionOverseer` (implements cross-partition UPDATEs)
- `PartitionRouter` (implements cross-partition UPDATEs)

`PartitionFilter` acts as a *proxy node* for INSERT's child scan, which means it can redirect output tuples to the corresponding partition:

```plpgsql
EXPLAIN (COSTS OFF)
INSERT INTO partitioned_table
SELECT generate_series(1, 10), random();
               QUERY PLAN
-----------------------------------------
 Insert on partitioned_table
   ->  Custom Scan (PartitionFilter)
         ->  Subquery Scan on "*SELECT*"
               ->  Result
(4 rows)
```

`PartitionOverseer` and `PartitionRouter` are another *proxy nodes* used
in conjunction with `PartitionFilter` to enable cross-partition UPDATEs
(i.e. when update of partitioning key requires that we move row to another
partition). Since this node has a great deal of side effects (ordinary `UPDATE` becomes slower;
cross-partition `UPDATE` is transformed into `DELETE + INSERT`),
it is disabled by default.
To enable it, refer to the list of [GUCs](#disabling-pg_pathman) below.

```plpgsql
EXPLAIN (COSTS OFF)
UPDATE partitioned_table
SET value = value + 1 WHERE value = 2;
                       QUERY PLAN
---------------------------------------------------------
 Custom Scan (PartitionOverseer)
   ->  Update on partitioned_table_2
         ->  Custom Scan (PartitionFilter)
               ->  Custom Scan (PartitionRouter)
                     ->  Seq Scan on partitioned_table_2
                           Filter: (value = 2)
(6 rows)
```

`RuntimeAppend` and `RuntimeMergeAppend` have much in common: they come in handy in a case when WHERE condition takes form of:
```
VARIABLE OP PARAM
```
This kind of expressions can no longer be optimized at planning time since the parameter's value is not known until the execution stage takes place. The problem can be solved by embedding the *WHERE condition analysis routine* into the original `Append`'s code, thus making it pick only required scans out of a whole bunch of planned partition scans. This effectively boils down to creation of a custom node capable of performing such a check.

----------

There are at least several cases that demonstrate usefulness of these nodes:

```plpgsql
/* create table we're going to partition */
CREATE TABLE partitioned_table(id INT NOT NULL, payload REAL);

/* insert some data */
INSERT INTO partitioned_table
SELECT generate_series(1, 1000), random();

/* perform partitioning */
SELECT create_hash_partitions('partitioned_table', 'id', 100);

/* create ordinary table */
CREATE TABLE some_table AS SELECT generate_series(1, 100) AS VAL;
```


 - **`id = (select ... limit 1)`**
```plpgsql
EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = (SELECT * FROM some_table LIMIT 1);
                                             QUERY PLAN
----------------------------------------------------------------------------------------------------
 Custom Scan (RuntimeAppend) (actual time=0.030..0.033 rows=1 loops=1)
   InitPlan 1 (returns $0)
     ->  Limit (actual time=0.011..0.011 rows=1 loops=1)
           ->  Seq Scan on some_table (actual time=0.010..0.010 rows=1 loops=1)
   ->  Seq Scan on partitioned_table_70 partitioned_table (actual time=0.004..0.006 rows=1 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 9
 Planning time: 1.131 ms
 Execution time: 0.075 ms
(9 rows)

/* disable RuntimeAppend node */
SET pg_pathman.enable_runtimeappend = f;

EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = (SELECT * FROM some_table LIMIT 1);
                                    QUERY PLAN
----------------------------------------------------------------------------------
 Append (actual time=0.196..0.274 rows=1 loops=1)
   InitPlan 1 (returns $0)
     ->  Limit (actual time=0.005..0.005 rows=1 loops=1)
           ->  Seq Scan on some_table (actual time=0.003..0.003 rows=1 loops=1)
   ->  Seq Scan on partitioned_table_0 (actual time=0.014..0.014 rows=0 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 6
   ->  Seq Scan on partitioned_table_1 (actual time=0.003..0.003 rows=0 loops=1)
         Filter: (id = $0)
         Rows Removed by Filter: 5
         ... /* more plans follow */
 Planning time: 1.140 ms
 Execution time: 0.855 ms
(306 rows)
```

 - **`id = ANY (select ...)`**
```plpgsql
EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = any (SELECT * FROM some_table limit 4);
                                                QUERY PLAN
-----------------------------------------------------------------------------------------------------------
 Nested Loop (actual time=0.025..0.060 rows=4 loops=1)
   ->  Limit (actual time=0.009..0.011 rows=4 loops=1)
         ->  Seq Scan on some_table (actual time=0.008..0.010 rows=4 loops=1)
   ->  Custom Scan (RuntimeAppend) (actual time=0.002..0.004 rows=1 loops=4)
         ->  Seq Scan on partitioned_table_70 partitioned_table (actual time=0.001..0.001 rows=10 loops=1)
         ->  Seq Scan on partitioned_table_26 partitioned_table (actual time=0.002..0.003 rows=9 loops=1)
         ->  Seq Scan on partitioned_table_27 partitioned_table (actual time=0.001..0.002 rows=20 loops=1)
         ->  Seq Scan on partitioned_table_63 partitioned_table (actual time=0.001..0.002 rows=9 loops=1)
 Planning time: 0.771 ms
 Execution time: 0.101 ms
(10 rows)

/* disable RuntimeAppend node */
SET pg_pathman.enable_runtimeappend = f;

EXPLAIN (COSTS OFF, ANALYZE) SELECT * FROM partitioned_table
WHERE id = any (SELECT * FROM some_table limit 4);
                                       QUERY PLAN
-----------------------------------------------------------------------------------------
 Nested Loop Semi Join (actual time=0.531..1.526 rows=4 loops=1)
   Join Filter: (partitioned_table.id = some_table.val)
   Rows Removed by Join Filter: 3990
   ->  Append (actual time=0.190..0.470 rows=1000 loops=1)
         ->  Seq Scan on partitioned_table (actual time=0.187..0.187 rows=0 loops=1)
         ->  Seq Scan on partitioned_table_0 (actual time=0.002..0.004 rows=6 loops=1)
         ->  Seq Scan on partitioned_table_1 (actual time=0.001..0.001 rows=5 loops=1)
         ->  Seq Scan on partitioned_table_2 (actual time=0.002..0.004 rows=14 loops=1)
... /* 96 scans follow */
   ->  Materialize (actual time=0.000..0.000 rows=4 loops=1000)
         ->  Limit (actual time=0.005..0.006 rows=4 loops=1)
               ->  Seq Scan on some_table (actual time=0.003..0.004 rows=4 loops=1)
 Planning time: 2.169 ms
 Execution time: 2.059 ms
(110 rows)
```

 - **`NestLoop` involving a partitioned table**, which is omitted since it's occasionally shown above.

----------

In case you're interested, you can read more about custom nodes at Alexander Korotkov's [blog](http://akorotkov.github.io/blog/2016/06/15/pg_pathman-runtime-append/).


## Examples

### Common tips
- You can easily add **_partition_** column containing the names of the underlying partitions using the system attribute called **_tableoid_**:
```plpgsql
SELECT tableoid::regclass AS partition, * FROM partitioned_table;
```

- Though indices on a parent table aren't particularly useful (since it's supposed to be empty), they act as prototypes for indices on partitions. For each index on the parent table, `pg_pathman` will create a similar index on every partition.

- All running concurrent partitioning tasks can be listed using the `pathman_concurrent_part_tasks` view:
```plpgsql
SELECT * FROM pathman_concurrent_part_tasks;
 userid | pid  | dbid  | relid | processed | status  
--------+------+-------+-------+-----------+---------
 dmitry | 7367 | 16384 | test  |    472000 | working
(1 row)
```

- `pathman_partition_list` in conjunction with `drop_range_partition()` can be used to drop RANGE partitions in a more flexible way compared to good old `DROP TABLE`:
```plpgsql
SELECT drop_range_partition(partition, false) /* move data to parent */
FROM pathman_partition_list
WHERE parent = 'part_test'::regclass AND range_min::int < 500;
NOTICE:  1 rows copied from part_test_11
NOTICE:  100 rows copied from part_test_1
NOTICE:  100 rows copied from part_test_2
 drop_range_partition 
----------------------
 dummy_test_11
 dummy_test_1
 dummy_test_2
(3 rows)
```

- You can turn foreign tables into partitions using the `attach_range_partition()` function. Rows that were meant to be inserted into parent will be redirected to foreign partitions (as usual, PartitionFilter will be involved), though by default it is prohibited to insert rows into partitions provided not by `postgres_fdw`. Only superuser is allowed to set `pg_pathman.insert_into_fdw` [GUC](#disabling-pg_pathman) variable.

### HASH partitioning
Consider an example of HASH partitioning. First create a table with some integer column:
```plpgsql
CREATE TABLE items (
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    code     BIGINT);

INSERT INTO items (id, name, code)
SELECT g, md5(g::text), random() * 100000
FROM generate_series(1, 100000) as g;
```
Now run the `create_hash_partitions()` function with appropriate arguments:
```plpgsql
SELECT create_hash_partitions('items', 'id', 100);
```
This will create new partitions and move the data from parent to partitions.

Here's an example of the query performing filtering by partitioning key:
```plpgsql
SELECT * FROM items WHERE id = 1234;
  id  |               name               | code
------+----------------------------------+------
 1234 | 81dc9bdb52d04dc20036dbd8313ed055 | 1855
(1 row)

EXPLAIN SELECT * FROM items WHERE id = 1234;
                                     QUERY PLAN
------------------------------------------------------------------------------------
 Append  (cost=0.28..8.29 rows=0 width=0)
   ->  Index Scan using items_34_pkey on items_34  (cost=0.28..8.29 rows=0 width=0)
         Index Cond: (id = 1234)
```

Notice that the `Append` node contains only one child scan which corresponds to the WHERE clause.

> **Important:** pay attention to the fact that `pg_pathman` excludes the parent table from the query plan.

To access parent table use ONLY modifier:
```plpgsql
EXPLAIN SELECT * FROM ONLY items;
                      QUERY PLAN
------------------------------------------------------
 Seq Scan on items  (cost=0.00..0.00 rows=1 width=45)
```
### RANGE partitioning
Consider an example of RANGE partitioning. Let's create a table containing some dummy logs:
```plpgsql
CREATE TABLE journal (
    id      SERIAL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT);

-- similar index will also be created for each partition
CREATE INDEX ON journal(dt);

-- generate some data
INSERT INTO journal (dt, level, msg)
SELECT g, random() * 6, md5(g::text)
FROM generate_series('2015-01-01'::date, '2015-12-31'::date, '1 minute') as g;
```
Run the `create_range_partitions()` function to create partitions so that each partition would contain the data for one day:
```plpgsql
SELECT create_range_partitions('journal', 'dt', '2015-01-01'::date, '1 day'::interval);
```
It will create 365 partitions and move the data from parent to partitions.

New partitions are appended automaticaly by insert trigger, but it can be done manually with the following functions:
```plpgsql
-- add new partition with specified range
SELECT add_range_partition('journal', '2016-01-01'::date, '2016-01-07'::date);

-- append new partition with default range
SELECT append_range_partition('journal');
```
The first one creates a partition with specified range. The second one creates a partition with default interval and appends it to the partition list. It is also possible to attach an existing table as partition. For example, we may want to attach an archive table (or even foreign table from another server) for some outdated data:
```plpgsql
CREATE FOREIGN TABLE journal_archive (
    id      INTEGER NOT NULL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT)
SERVER archive_server;

SELECT attach_range_partition('journal', 'journal_archive', '2014-01-01'::date, '2015-01-01'::date);
```
> **Important:** the definition of the attached table must match the one of the existing partitioned table, including the dropped columns.

To merge to adjacent partitions, use the `merge_range_partitions()` function:
```plpgsql
SELECT merge_range_partitions('journal_archive', 'journal_1');
```
To split partition by value, use the `split_range_partition()` function:
```plpgsql
SELECT split_range_partition('journal_366', '2016-01-03'::date);
```
To detach partition, use the `detach_range_partition()` function:
```plpgsql
SELECT detach_range_partition('journal_archive');
```

Here's an example of the query performing filtering by partitioning key:
```plpgsql
SELECT * FROM journal WHERE dt >= '2015-06-01' AND dt < '2015-06-03';
   id   |         dt          | level |               msg
--------+---------------------+-------+----------------------------------
 217441 | 2015-06-01 00:00:00 |     2 | 15053892d993ce19f580a128f87e3dbf
 217442 | 2015-06-01 00:01:00 |     1 | 3a7c46f18a952d62ce5418ac2056010c
 217443 | 2015-06-01 00:02:00 |     0 | 92c8de8f82faf0b139a3d99f2792311d
 ...
(2880 rows)

EXPLAIN SELECT * FROM journal WHERE dt >= '2015-06-01' AND dt < '2015-06-03';
                            QUERY PLAN
------------------------------------------------------------------
 Append  (cost=0.00..58.80 rows=0 width=0)
   ->  Seq Scan on journal_152  (cost=0.00..29.40 rows=0 width=0)
   ->  Seq Scan on journal_153  (cost=0.00..29.40 rows=0 width=0)
(3 rows)
```

### Disabling `pg_pathman`
There are several user-accessible [GUC](https://www.postgresql.org/docs/9.5/static/config-setting.html) variables designed to toggle the whole module or specific custom nodes on and off:

 - `pg_pathman.enable` --- disable (or enable) `pg_pathman` **completely**
 - `pg_pathman.enable_runtimeappend` --- toggle `RuntimeAppend` custom node on\off
 - `pg_pathman.enable_runtimemergeappend` --- toggle `RuntimeMergeAppend` custom node on\off
 - `pg_pathman.enable_partitionfilter` --- toggle `PartitionFilter` custom node on\off (for INSERTs)
 - `pg_pathman.enable_partitionrouter` --- toggle `PartitionRouter` custom node on\off (for cross-partition UPDATEs)
 - `pg_pathman.enable_auto_partition` --- toggle automatic partition creation on\off (per session)
 - `pg_pathman.enable_bounds_cache` --- toggle bounds cache on\off (faster updates of partitioning scheme)
 - `pg_pathman.insert_into_fdw` --- allow INSERTs into various FDWs `(disabled | postgres | any_fdw)`
 - `pg_pathman.override_copy` --- toggle COPY statement hooking on\off

To **permanently** disable `pg_pathman` for some previously partitioned table, use the `disable_pathman_for()` function:
```plpgsql
SELECT disable_pathman_for('range_rel');
```
All sections and data will remain unchanged and will be handled by the standard PostgreSQL inheritance mechanism.

## Feedback
Do not hesitate to post your issues, questions and new ideas at the [issues](https://github.com/postgrespro/pg_pathman/issues) page.

## Authors
[Ildar Musin](https://github.com/zilder)  
Alexander Korotkov <a.korotkov(at)postgrespro.ru> Postgres Professional Ltd., Russia  
[Dmitry Ivanov](https://github.com/funbringer)  
Maksim Milyutin <m.milyutin(at)postgrespro.ru> Postgres Professional Ltd., Russia  
[Ildus Kurbangaliev](https://github.com/ildus)
