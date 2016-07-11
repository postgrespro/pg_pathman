[![Build Status](https://travis-ci.org/postgrespro/pg_pathman.svg?branch=master)](https://travis-ci.org/postgrespro/pg_pathman)

# pg_pathman

The `pg_pathman` module provides optimized partitioning mechanism and functions to manage partitions.

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
* **HASH** - maps rows to partitions using a generic hash function (only *integer* attributes are supported at the moment).

More interesting features are yet to come. Stay tuned!

## Roadmap
 * Replace INSERT triggers with a custom node (aka **PartitionFilter**)
 * Implement [concurrent partitioning](https://github.com/postgrespro/pg_pathman/tree/concurrent_part) (much more responsive)
 * Implement HASH partitioning for non-integer attributes
 * Optimize hash join (both tables are partitioned by join key)
 * Implement LIST partitioning scheme

## Installation guide
To install `pg_pathman`, execute this in the module's directory:
```
make install USE_PGXS=1
```
Modify the **`shared_preload_libraries`** parameter in `postgresql.conf` as following:
```
shared_preload_libraries = 'pg_pathman'
```
It is essential to restart the PostgreSQL instance. After that, execute the following query in psql:
```
CREATE EXTENSION pg_pathman;
```

Done! Now it's time to setup your partitioning schemes.

> **Important:** Don't forget to set the `PG_CONFIG` variable in case you want to test `pg_pathman` on a custom build of PostgreSQL. Read more [here](https://wiki.postgresql.org/wiki/Building_and_Installing_PostgreSQL_Extension_Modules).

## Available functions

### Partition creation
```plpgsql
create_hash_partitions(relation         TEXT,
                       attribute        TEXT,
                       partitions_count INTEGER)
```
Performs HASH partitioning for `relation` by integer key `attribute`. Creates `partitions_count` partitions and trigger on INSERT. All the data will be automatically copied from the parent to partitions.

```plpgsql
create_range_partitions(relation    TEXT,
                        attribute   TEXT,
                        start_value ANYELEMENT,
                        interval    ANYELEMENT,
                        premake     INTEGER DEFAULT NULL)

create_range_partitions(relation    TEXT,
                        attribute   TEXT,
                        start_value ANYELEMENT,
                        interval    INTERVAL,
                        premake     INTEGER DEFAULT NULL)
```
Performs RANGE partitioning for `relation` by partitioning key `attribute`. `start_value` argument specifies initial value, `interval` sets the range of values in a single partition, `premake` is the number of premade partitions (if not set then pathman tries to determine it based on attribute values). All the data will be automatically copied from the parent to partitions.

```plpgsql
create_partitions_from_range(relation    TEXT,
                             attribute   TEXT,
                             start_value ANYELEMENT,
                             end_value   ANYELEMENT,
                             interval    ANYELEMENT)

create_partitions_from_range(relation    TEXT,
                             attribute   TEXT,
                             start_value ANYELEMENT,
                             end_value   ANYELEMENT,
                             interval    INTERVAL)
```
Performs RANGE-partitioning from specified range for `relation` by partitioning key `attribute`. Data will be copied to partitions as well.

### Triggers
```plpgsql
create_hash_update_trigger(parent TEXT)
```
Creates the trigger on UPDATE for HASH partitions. The UPDATE trigger isn't created by default because of the overhead. It's useful in cases when the key attribute might change.
```plpgsql
create_range_update_trigger(parent TEXT)
```
Same as above, but for a RANGE-partitioned table.

### Post-creation partition management
```plpgsql
split_range_partition(partition TEXT, value ANYELEMENT)
```
Split RANGE `partition` in two by `value`.

```plpgsql
merge_range_partitions(partition1 TEXT, partition2 TEXT)
```
Merge two adjacent RANGE partitions. First, data from `partition2` is copied to `partition1`, then `partition2` is removed.

```plpgsql
append_range_partition(p_relation TEXT)
```
Append new RANGE partition.

```plpgsql
prepend_range_partition(p_relation TEXT)
```
Prepend new RANGE partition.

```plpgsql
add_range_partition(relation    TEXT,
                    start_value ANYELEMENT,
                    end_value   ANYELEMENT)
```
Create new RANGE partition for `relation` with specified range bounds.

```plpgsql
drop_range_partition(partition TEXT)
```
Drop RANGE partition and all its data.

```plpgsql
attach_range_partition(relation    TEXT,
                       partition   TEXT,
                       start_value ANYELEMENT,
                       end_value   ANYELEMENT)
```
Attach partition to the existing RANGE-partitioned relation. The attached table must have exactly the same structure as the parent table, including the dropped columns.

```plpgsql
detach_range_partition(partition TEXT)
```
Detach partition from the existing RANGE-partitioned relation.

```plpgsql
disable_partitioning(relation TEXT)
```
Permanently disable `pg_pathman` partitioning mechanism for the specified parent table and remove the insert trigger if it exists. All partitions and data remain unchanged.

## Custom plan nodes
`pg_pathman` provides a couple of [custom plan nodes](https://wiki.postgresql.org/wiki/CustomScanAPI) which aim to reduce execution time, namely:

- `RuntimeAppend` (overrides `Append` plan node)
- `RuntimeMergeAppend` (overrides `MergeAppend` plan node)

`RuntimeAppend` and `RuntimeMergeAppend` have much in common: they come in handy in a case when WHERE condition takes form of:
```
VARIABLE OP PARAM
```
This kind of expressions can no longer be optimized at planning time since the parameter's value is not known until the execution stage takes place. The problem can be solved by embedding the *WHERE condition analysis routine* into the original `Append`'s code, thus making it pick only required scans out of a whole bunch of planned partition scans. This effectively boils down to creation of a custom node capable of performing such a check.

----------

There are at least several cases that demonstrate usefulness of these nodes:

```
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
```
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
```
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
```
SELECT tableoid::regclass AS partition, * FROM partitioned_table;
```

- Though indices on a parent table aren't particularly useful (since it's empty), they act as prototypes for indices on partitions. For each index on the parent table, `pg_pathman` will create a similar index on every partition.

### HASH partitioning
Consider an example of HASH partitioning. First create a table with some integer column:
```
CREATE TABLE items (
    id       SERIAL PRIMARY KEY,
    name     TEXT,
    code     BIGINT);

INSERT INTO items (id, name, code)
SELECT g, md5(g::text), random() * 100000
FROM generate_series(1, 100000) as g;
```
Now run the `create_hash_partitions()` function with appropriate arguments:
```
SELECT create_hash_partitions('items', 'id', 100);
```
This will create new partitions and move the data from parent to partitions.

Here's an example of the query performing filtering by partitioning key:
```
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
```
EXPLAIN SELECT * FROM ONLY items;
                      QUERY PLAN
------------------------------------------------------
 Seq Scan on items  (cost=0.00..0.00 rows=1 width=45)
```
### RANGE partitioning
Consider an example of RANGE partitioning. Let's create a table containing some dummy logs:
```
CREATE TABLE journal (
    id      SERIAL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
);

-- similar index will also be created for each partition
CREATE INDEX ON journal(dt);

-- generate some data
INSERT INTO journal (dt, level, msg)
SELECT g, random() * 6, md5(g::text)
FROM generate_series('2015-01-01'::date, '2015-12-31'::date, '1 minute') as g;
```
Run the `create_range_partitions()` function to create partitions so that each partition would contain the data for one day:
```
SELECT create_range_partitions('journal', 'dt', '2015-01-01'::date, '1 day'::interval);
```
It will create 365 partitions and move the data from parent to partitions.

New partitions are appended automaticaly by insert trigger, but it can be done manually with the following functions:
```
-- append new partition with specified range
SELECT add_range_partition('journal', '2016-01-01'::date, '2016-01-07'::date);

-- append new partition with default range
SELECT append_range_partition('journal');
```
The first one creates a partition with specified range. The second one creates a partition with default interval and appends it to the partition list. It is also possible to attach an existing table as partition. For example, we may want to attach an archive table (or even foreign table from another server) for some outdated data:
```
CREATE FOREIGN TABLE journal_archive (
    id      INTEGER NOT NULL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
) SERVER archive_server;

SELECT attach_range_partition('journal', 'journal_archive', '2014-01-01'::date, '2015-01-01'::date);
```
> **Important:** the definition of the attached table must match the one of the existing partitioned table, including the dropped columns.

To merge to adjacent partitions, use the `merge_range_partitions()` function:
```
SELECT merge_range_partitions('journal_archive', 'journal_1');
```
To split partition by value, use the `split_range_partition()` function:
```
SELECT split_range_partition('journal_366', '2016-01-03'::date);
```
To detach partition, use the `detach_range_partition()` function:
```
SELECT detach_range_partition('journal_archive');
```

Here's an example of the query performing filtering by partitioning key:
```
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

To **permanently** disable `pg_pathman` for some previously partitioned table, use the `disable_partitioning()` function:
```
SELECT disable_partitioning('range_rel');
```
All sections and data will remain unchanged and will be handled by the standard PostgreSQL inheritance mechanism.

##Feedback
Do not hesitate to post your issues, questions and new ideas at the [issues](https://github.com/postgrespro/pg_pathman/issues) page.

## Authors
Ildar Musin <i.musin@postgrespro.ru> Postgres Professional Ltd., Russia     
Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia       
Dmitry Ivanov <d.ivanov@postgrespro.ru> Postgres Professional Ltd., Russia      
