[![Build Status](https://travis-ci.org/postgrespro/pg_pathman.svg?branch=master)](https://travis-ci.org/postgrespro/pg_pathman)
[![PGXN version](https://badge.fury.io/pg/pg_pathman.svg)](https://badge.fury.io/pg/pg_pathman)

# pg_pathman

The `pg_pathman` module provides optimized partitioning mechanism and functions to manage partitions.

The extension is compatible with PostgreSQL 9.5 (9.6 support is coming soon).

## Overview
**Partitioning** means splitting one large table into smaller pieces. Each row in such table is moved to a single partition according to the partitioning key. PostgreSQL supports partitioning via table inheritance: each partition must be created as a child table with CHECK CONSTRAINT. For example:

```
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

Despite the flexibility, this approach forces the planner to perform an exhaustive search and to check constraints on each partition to determine whether it should be present in the plan or not. Large amount of partitions may result in significant planning overhead.

The `pg_pathman` module features partition managing functions and optimized planning mechanism which utilizes knowledge of the partitions' structure. It stores partitioning configuration in the `pathman_config` table; each row contains a single entry for a partitioned table (relation name, partitioning column and its type). During the initialization stage the `pg_pathman` module caches some information about child partitions in the shared memory, which is used later for plan construction. Before a SELECT query is executed, `pg_pathman` traverses the condition tree in search of expressions like:

```
VARIABLE OP CONST
```
where `VARIABLE` is a partitioning key, `OP` is a comparison operator (supported operators are =, <, <=, >, >=), `CONST` is a scalar value. For example:

```
WHERE id = 150
```

Based on the partitioning type and condition's operator, `pg_pathman` searches for the corresponding partitions and builds the plan. Currently `pg_pathman` supports two partitioning schemes:

* **RANGE** - maps rows to partitions using partitioning key ranges assigned to each partition. Optimization is achieved by using the binary search algorithm;
* **HASH** - maps rows to partitions using a generic hash function.

More interesting features are yet to come. Stay tuned!

## Roadmap

 * Implement LIST partitioning scheme;
 * Optimize hash join (both tables are partitioned by join key).

## Installation guide
To install `pg_pathman`, execute this in the module's directory:
```shell
make install USE_PGXS=1
```
Modify the **`shared_preload_libraries`** parameter in `postgresql.conf` as following:
```
shared_preload_libraries = 'pg_pathman'
```
It is essential to restart the PostgreSQL instance. After that, execute the following query in psql:
```plpgsql
CREATE EXTENSION pg_pathman;
```

Done! Now it's time to setup your partitioning schemes.

> **Important:** Don't forget to set the `PG_CONFIG` variable in case you want to test `pg_pathman` on a custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

## Available functions

### Partition creation
```plpgsql
create_hash_partitions(relation         REGCLASS,
                       attribute        TEXT,
                       partitions_count INTEGER,
                       partition_name   TEXT DEFAULT NULL,
                       partition_data   BOOLEAN DEFAULT TRUE)
```
Performs HASH partitioning for `relation` by integer key `attribute`. The `partitions_count` parameter specifies the number of partitions to create; it cannot be changed afterwards. If `partition_data` is `true` then all the data will be automatically copied from the parent table to partitions. Note that data migration may took a while to finish and the table will be locked until transaction commits. See `partition_table_concurrently()` for a lock-free way to migrate data. Partition creation callback is invoked for each partition if set beforehand (see `set_part_init_callback()`).

```plpgsql
create_range_partitions(relation       REGCLASS,
                        attribute      TEXT,
                        start_value    ANYELEMENT,
                        interval       ANYELEMENT,
                        count          INTEGER DEFAULT NULL
                        partition_data BOOLEAN DEFAULT TRUE)

create_range_partitions(relation       REGCLASS,
                        attribute      TEXT,
                        start_value    ANYELEMENT,
                        interval       INTERVAL,
                        count          INTEGER DEFAULT NULL,
                        partition_data BOOLEAN DEFAULT TRUE)
```
Performs RANGE partitioning for `relation` by partitioning key `attribute`. `start_value` argument specifies initial value, `interval` sets the range of values in a single partition, `count` is the number of premade partitions (if not set then pathman tries to determine it based on attribute values). Partition creation callback is invoked for each partition if set beforehand.

```plpgsql
create_partitions_from_range(relation       REGCLASS,
                             attribute      TEXT,
                             start_value    ANYELEMENT,
                             end_value      ANYELEMENT,
                             interval       ANYELEMENT,
                             partition_data BOOLEAN DEFAULT TRUE)

create_partitions_from_range(relation       REGCLASS,
                             attribute      TEXT,
                             start_value    ANYELEMENT,
                             end_value      ANYELEMENT,
                             interval       INTERVAL,
                             partition_data BOOLEAN DEFAULT TRUE)
```
Performs RANGE-partitioning from specified range for `relation` by partitioning key `attribute`. Partition creation callback is invoked for each partition if set beforehand.

### Data migration

```plpgsql
partition_table_concurrently(relation REGCLASS)
```
Starts a background worker to move data from parent table to partitions. The worker utilizes short transactions to copy small batches of data (up to 10K rows per transaction) and thus doesn't significantly interfere with user's activity.

```plpgsql
stop_concurrent_part_task(relation REGCLASS)
```
Stops a background worker performing a concurrent partitioning task. Note: worker will exit after it finishes relocating a current batch.

### Triggers
```plpgsql
create_hash_update_trigger(parent REGCLASS)
```
Creates the trigger on UPDATE for HASH partitions. The UPDATE trigger isn't created by default because of the overhead. It's useful in cases when the key attribute might change.
```plpgsql
create_range_update_trigger(parent REGCLASS)
```
Same as above, but for a RANGE-partitioned table.

### Post-creation partition management
```plpgsql
split_range_partition(partition      REGCLASS,
                      value          ANYELEMENT,
                      partition_name TEXT DEFAULT NULL)
```
Split RANGE `partition` in two by `value`. Partition creation callback is invoked for a new partition if available.

```plpgsql
merge_range_partitions(partition1 REGCLASS, partition2 REGCLASS)
```
Merge two adjacent RANGE partitions. First, data from `partition2` is copied to `partition1`, then `partition2` is removed.

```plpgsql
append_range_partition(p_relation     REGCLASS,
                       partition_name TEXT DEFAULT NULL,
                       tablespace     TEXT DEFAULT NULL)
```
Append new RANGE partition with `pathman_config.range_interval` as interval.

```plpgsql
prepend_range_partition(p_relation     REGCLASS,
                        partition_name TEXT DEFAULT NULL,
                        tablespace     TEXT DEFAULT NULL)
```
Prepend new RANGE partition with `pathman_config.range_interval` as interval.

```plpgsql
add_range_partition(relation       REGCLASS,
                    start_value    ANYELEMENT,
                    end_value      ANYELEMENT,
                    partition_name TEXT DEFAULT NULL,
                    tablespace     TEXT DEFAULT NULL)
```
Create new RANGE partition for `relation` with specified range bounds.

```plpgsql
drop_range_partition(partition TEXT, delete_data BOOLEAN DEFAULT TRUE)
```
Drop RANGE partition and all of its data if `delete_data` is true.

```plpgsql
attach_range_partition(relation    REGCLASS,
                       partition   REGCLASS,
                       start_value ANYELEMENT,
                       end_value   ANYELEMENT)
```
Attach partition to the existing RANGE-partitioned relation. The attached table must have exactly the same structure as the parent table, including the dropped columns. Partition creation callback is invoked if set (see `pathman_config_params`).

```plpgsql
detach_range_partition(partition REGCLASS)
```
Detach partition from the existing RANGE-partitioned relation.

```plpgsql
disable_pathman_for(relation TEXT)
```
Permanently disable `pg_pathman` partitioning mechanism for the specified parent table and remove the insert trigger if it exists. All partitions and data remain unchanged.

```plpgsql
drop_partitions(parent      REGCLASS,
                delete_data BOOLEAN DEFAULT FALSE)
```
Drop partitions of the `parent` table (both foreign and local relations). If `delete_data` is `false`, the data is copied to the parent table first. Default is `false`.


### Additional parameters

```plpgsql
set_enable_parent(relation REGCLASS, value BOOLEAN)
```
Include/exclude parent table into/from query plan. In original PostgreSQL planner parent table is always included into query plan even if it's empty which can lead to additional overhead. You can use `disable_parent()` if you are never going to use parent table as a storage. Default value depends on the `partition_data` parameter that was specified during initial partitioning in `create_range_partitions()` or `create_partitions_from_range()` functions. If the `partition_data` parameter was `true` then all data have already been migrated to partitions and parent table disabled. Otherwise it is enabled.

```plpgsql
set_auto(relation REGCLASS, value BOOLEAN)
```
Enable/disable auto partition propagation (only for RANGE partitioning). It is enabled by default.

```plpgsql
set_init_callback(relation REGCLASS, callback REGPROC DEFAULT 0)
```
Set partition creation callback to be invoked for each attached or created partition (both HASH and RANGE). The callback must have the following signature: `part_init_callback(args JSONB) RETURNS VOID`. Parameter `arg` consists of several fields whose presence depends on partitioning type:
```json
/* RANGE-partitioned table abc (child abc_4) */
{
    "parent":    "abc",
    "parttype":  "2",
    "partition": "abc_4",
    "range_max": "401",
    "range_min": "301"
}

/* HASH-partitioned table abc (child abc_0) */
{
    "parent":    "abc",
    "parttype":  "1",
    "partition": "abc_0"
}
```

## Views and tables

#### `pathman_config` --- main config storage
```plpgsql
CREATE TABLE IF NOT EXISTS pathman_config (
    partrel         REGCLASS NOT NULL PRIMARY KEY,
    attname         TEXT NOT NULL,
    parttype        INTEGER NOT NULL,
    range_interval  TEXT,

    CHECK (parttype IN (1, 2)) /* check for allowed part types */ );
```
This table stores a list of partitioned tables.

#### `pathman_config_params` --- optional parameters
```plpgsql
CREATE TABLE IF NOT EXISTS pathman_config_params (
    partrel        REGCLASS NOT NULL PRIMARY KEY,
    enable_parent  BOOLEAN NOT NULL DEFAULT TRUE,
    auto           BOOLEAN NOT NULL DEFAULT TRUE,
    init_callback  REGPROCEDURE NOT NULL DEFAULT 0);
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
    partattr   TEXT,
    range_min  TEXT,
    range_max  TEXT)
AS 'pg_pathman', 'show_partition_list_internal'
LANGUAGE C STRICT;

CREATE OR REPLACE VIEW pathman_partition_list
AS SELECT * FROM show_partition_list();
```
This view lists all existing partitions, as well as their parents and range boundaries (NULL for HASH partitions).


## Custom plan nodes
`pg_pathman` provides a couple of [custom plan nodes](https://wiki.postgresql.org/wiki/CustomScanAPI) which aim to reduce execution time, namely:

- `RuntimeAppend` (overrides `Append` plan node)
- `RuntimeMergeAppend` (overrides `MergeAppend` plan node)
- `PartitionFilter` (drop-in replacement for INSERT triggers)

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

 - `pg_pathman.enable` --- disable (or enable) `pg_pathman` completely
 - `pg_pathman.enable_runtimeappend` --- toggle `RuntimeAppend` custom node on\off
 - `pg_pathman.enable_runtimemergeappend` --- toggle `RuntimeMergeAppend` custom node on\off
 - `pg_pathman.enable_partitionfilter` --- toggle `PartitionFilter` custom node on\off
 - `pg_pathman.enable_auto_partition` --- toggle automatic partition creation on\off (per session)
 - `pg_pathman.insert_into_fdw` --- allow INSERTs into various FDWs `(disabled | postgres | any_fdw)`
 - `pg_pathman.override_copy` --- toggle COPY statement hooking on\off

To **permanently** disable `pg_pathman` for some previously partitioned table, use the `disable_pathman_for()` function:
```
SELECT disable_pathman_for('range_rel');
```
All sections and data will remain unchanged and will be handled by the standard PostgreSQL inheritance mechanism.

##Feedback
Do not hesitate to post your issues, questions and new ideas at the [issues](https://github.com/postgrespro/pg_pathman/issues) page.

## Authors
Ildar Musin <i.musin@postgrespro.ru> Postgres Professional Ltd., Russia		
Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia		
Dmitry Ivanov <d.ivanov@postgrespro.ru> Postgres Professional Ltd., Russia		
