# pg_pathman

The `pg_pathman` module provides optimized partitioning mechanism and functions to manage partitions.

## pg_pathman Concepts

Partitioning refers to splitting one large table into smaller pieces. Each row in such table assigns to a single partition based on partitioning key. Common partitioning strategies are:

* HASH - maps rows to partitions based on hash function values;
* RANGE - maps data to partitions based on ranges that you establish for each partition;
* LIST - maps data to partitions based on explicitly specified values of partitioning key for each partition.

PostgreSQL supports partitioning via table inheritance. Each partition must be created as child table with CHECK CONSTRAINT. For example:

```
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

Despite the flexibility of this approach it has weakness. If query uses filtering the optimizer forced to perform an exhaustive search and check constraints for each partition to determine partitions from which it should select data. If the number of partitions is large the overhead may be significant.

The `pg_pathman` module provides functions to manage partitions and partitioning mechanism optimized based on knowledge of the partitions structure. It stores partitioning configuration in the `pathman_config` table, each row of which contains single entry for partitioned table (relation name, partitioning key and type). During initialization the `pg_pathman` module caches information about child partitions in shared memory in form convenient to perform rapid search. When user executes SELECT query pg_pathman analyzes conditions tree looking for conditions like:

```
VARIABLE OP CONST
```
where `VARIABLE` is partitioning key, `OP` is comparison operator (supported operators are =, <, <=, >, >=), `CONST` is scalar value. For example:

```
WHERE id = 150
```

Based on partitioning type and operator the `pg_pathman` searches corresponding partitions and builds the plan.

## Installation

To install pg_pathman run in pg_pathman directory:
```
make install USE_PGXS=1
```
Modify shared_preload_libraries parameter in postgres.conf as following:
```
shared_preload_libraries = 'pg_pathman'
```
It will require to restart the PostgreSQL instance. Then execute following query in psql:
```
CREATE EXTENSION pg_pathman;
```

## pg_pathman Functions

### Partitions Creation
```
create_hash_partitions(
    relation TEXT,
    attribute TEXT,
    partitions_count INTEGER)
```
Performs HASH partitioning for `relation` by integer key `attribute`. Creates `partitions_count` partitions and trigger on INSERT. Data doesn't automatically copied from parent table to partitions. Use `partition_data()` function (see below) to migrate data.

```
create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval ANYELEMENT,
    premake INTEGER)
```
Performs RANGE partitioning for `relation` by partitioning key `attribute`. `start_value` argument specifies initial value, `interval` sets the range of values in a single partition, `premake` is the number of premade partitions (the only one partition will be created if `premake` is 0).
```
create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval INTERVAL,
    premake INTEGER)
```
Same as above but suitable for `DATE` and `TIMESTAMP` partitioning keys.

### Utilities
```
partition_data(parent text)
```
Copies data from parent table to its partitions.
```
create_hash_update_trigger(parent TEXT)
```
Creates the trigger on UPDATE for HASH partitions. The UPDATE trigger isn't created by default because of overhead. It is useful in cases when key attribute could be changed.
```
create_hash_update_trigger(parent TEXT)
```
Same as above for RANGE sections.

### Partitions management
```
split_range_partition(partition TEXT, value ANYELEMENT)
```
Splits RANGE `partition` in two by `value`.
```
merge_range_partitions(partition1 TEXT, partition2 TEXT)
```
Merge two adjacent RANGE partitions. Data from `partition2` is copied to `partition1`. Then the `partition2` is removed.
```
append_partition(p_relation TEXT)
```
Appends new partition with the range equal to the range of the previous partition.
```
prepend_partition(p_relation TEXT)
```
Prepends new partition with the range equal to the range of the first partition.
```
disable_partitioning(relation TEXT)
```
Disables `pg_pathman` partitioning mechanism for the specified parent table and removes an insert trigger. Partitions itself remain unchanged.

## Examples
### HASH
Consider an example of HASH partitioning. First create a table with some integer column:
```
CREATE TABLE hash_rel (
    id      SERIAL PRIMARY KEY,
    value   INTEGER);
INSERT INTO hash_rel (value) SELECT g FROM generate_series(1, 10000) as g;
```
Then run create_hash_partitions() function with appropriate arguments:
```
SELECT create_hash_partitions('hash_rel', 'value', 100);
```
This will create new partitions but data will still be in the parent table. To move data to the corresponding partitions use partition_data() function:
```
SELECT partition_data('hash_rel');
```
Here is an example of the query with filtering by partitioning key and its plan:
```
SELECT * FROM hash_rel WHERE value = 1234;
  id  | value 
------+-------
 1234 |  1234

EXPLAIN SELECT * FROM hash_rel WHERE value = 1234;
                           QUERY PLAN                            
-----------------------------------------------------------------
 Append  (cost=0.00..2.00 rows=0 width=0)
   ->  Seq Scan on hash_rel_34  (cost=0.00..2.00 rows=0 width=0)
         Filter: (value = 1234)
```
Note that pg_pathman exludes parent table from the query plan. To access parent table use ONLY modifier:
```
EXPLAIN SELECT * FROM ONLY hash_rel;
                       QUERY PLAN                       
--------------------------------------------------------
 Seq Scan on hash_rel  (cost=0.00..0.00 rows=1 width=8)
```
### RANGE
Consider an example of RANGE partitioning. Create a table with numerical or date or timestamp column:
```
CREATE TABLE range_rel (
    id SERIAL PRIMARY KEY,
    dt TIMESTAMP);
INSERT INTO range_rel (dt) SELECT g FROM generate_series('2010-01-01'::date, '2014-12-31'::date, '1 day') as g;
```
Run create_range_partitions() function to create partitions so that each partition would contain data for one month:
```
SELECT create_range_partitions('range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 59);
```
It will create 60 partitions (one partition is created regardless of `premake` parameter). Now move data from the parent to partitions.
```
SELECT partition_data('range_rel');
```
To merge to adjacent partitions run merge_range_partitions() function:
```
SELECT merge_range_partitions('range_rel_1', 'range_rel_2');
```
To split partition use split_range_partition() function:
```
SELECT split_range_partition('range_rel_1', '2010-02-15'::date);
```
Now let's create new partition. You can use append_partition() or prepend_partition() functions:
```
SELECT append_partition('range_rel');
```
Here is an example of the query with filtering by partitioning key and its plan:
```
SELECT * FROM range_rel WHERE dt >= '2012-04-30' AND dt <= '2012-05-01';
 id  |         dt          
-----+---------------------
 851 | 2012-04-30 00:00:00
 852 | 2012-05-01 00:00:00

EXPLAIN SELECT * FROM range_rel WHERE dt >= '2012-04-30' AND dt <= '2012-05-01';
                                 QUERY PLAN                                 
----------------------------------------------------------------------------
 Append  (cost=0.00..60.80 rows=0 width=0)
   ->  Seq Scan on range_rel_28  (cost=0.00..30.40 rows=0 width=0)
         Filter: (dt >= '2012-04-30 00:00:00'::timestamp without time zone)
   ->  Seq Scan on range_rel_29  (cost=0.00..30.40 rows=0 width=0)
         Filter: (dt <= '2012-05-01 00:00:00'::timestamp without time zone)
```

### Disable pg_pathman
To disable pg_pathman for some previously partitioned table use disable_partitioning() function:
```
SELECT disable_partitioning('range_rel');
```
All sections and data will stay available and will be handled by standard PostgreSQL partitioning mechanism.
### Manual partitions management
It is possible to manage partitions manually. After creating or removing child tables it's necessary to invoke function:
```
on_update_partitions(oid),
```
which updates internal structures in memory of `pg_pathman module`. For example, let's create new section for the `range_rel` from above:
```
CREATE TABLE range_rel_archive (CHECK (dt >= '2000-01-01' AND dt < '2010-01-01')) INHERITS (range_rel);
SELECT on_update_partitions('range_rel'::regclass::oid);
```
CHECK CONSTRAINT must have the exact format:
* (VARIABLE >= CONST AND VARIABLE < CONST) for RANGE partitioned tables;
* (VARIABLE % CONST = CONST) for HASH partitioned tables.

It is possible to create partition from foreign table as well:
```
CREATE FOREIGN TABLE range_rel_archive (
    id INTEGER NOT NULL,
    dt TIMESTAMP)
SERVER archive_server;
ALTER TABLE range_rel_archive INHERIT range_rel;
ALTER TABLE range_rel_archive ADD CHECK (dt >= '2000-01-01' AND dt < '2010-01-01');
SELECT on_update_partitions('range_rel'::regclass::oid);
```
Foreign table structure must exactly match the parent table.

In case when parent table is being dropped by DROP TABLE, you should invoke on_remove_partitions() function and delete particular entry from `pathman_config` table:
```
SELECT on_remove_partitions('range_rel'::regclass::oid);
DROP TABLE range_rel CASCADE;
DELETE FROM pathman_config WHERE relname = 'public.range_rel';
```

## Author
Ildar Musin <i.musin@postgrespro.ru> Postgres Professional Ltd., Russia

This module is sponsored by Postgres Professional Ltd., Russia
