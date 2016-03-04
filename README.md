# pg_pathman

The `pg_pathman` module provides optimized partitioning mechanism and functions to manage partitions.

## pg_pathman Concepts

Partitioning refers to splitting one large table into smaller pieces. Each row in such table assigns to a single partition based on partitioning key. PostgreSQL supports partitioning via table inheritance. Each partition must be created as child table with CHECK CONSTRAINT. For example:

```
CREATE TABLE test (id SERIAL PRIMARY KEY, title TEXT);
CREATE TABLE test_1 (CHECK ( id >= 100 AND id < 200 )) INHERITS (test);
CREATE TABLE test_2 (CHECK ( id >= 200 AND id < 300 )) INHERITS (test);
```

Despite the flexibility this approach forces the planner to perform an exhaustive search and check constraints for each partition to determine which one should present in the plan. If the number of partitions is large the overhead may be significant.

The `pg_pathman` module provides functions to manage partitions and partitioning mechanism optimized based on knowledge of the partitions structure. It stores partitioning configuration in the `pathman_config` table, each row of which contains single entry for partitioned table (relation name, partitioning key and type). During initialization the `pg_pathman` module caches information about child partitions in shared memory in form convenient to perform rapid search. When SELECT query executes `pg_pathman` analyzes conditions tree looking for conditions like:

```
VARIABLE OP CONST
```
where `VARIABLE` is partitioning key, `OP` is comparison operator (supported operators are =, <, <=, >, >=), `CONST` is scalar value. For example:

```
WHERE id = 150
```

Based on partitioning type and operator the `pg_pathman` searches corresponding partitions and builds the plan. Current version of `pg_pathman` supports two partitioning types:

* RANGE - maps data to partitions based on ranges of partitioning key. Optimization is achieved by using binary search algorithm.
* HASH - maps rows to partitions based on hash function values (only INTEGER attributes at the moment);

## Roadmap

* Optimize the execution of the NestedLoop join method;
* LIST-partitioning;
* HASH-partitioning for non integer attributes.

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
Performs HASH partitioning for `relation` by integer key `attribute`. Creates `partitions_count` partitions and trigger on INSERT. All the data will be automatically copied from the parent to partitions.

```
create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval ANYELEMENT,
    premake INTEGER DEFAULT NULL)

create_range_partitions(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    interval INTERVAL,
    premake INTEGER DEFAULT NULL)
```
Performs RANGE partitioning for `relation` by partitioning key `attribute`. `start_value` argument specifies initial value, `interval` sets the range of values in a single partition, `premake` is the number of premade partitions (if not set then pathman tries to determine it based on attribute values). All the data will be automatically copied from the parent to partitions.

```
create_partitions_from_range(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT,
    interval ANYELEMENT)

create_partitions_from_range(
    relation TEXT,
    attribute TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT,
    interval INTERVAL)
```
Performs RANGE-partitioning from specified range for `relation` by partitioning key `attribute`. Data will be copied to partitions as well.

### Triggers
```
create_hash_update_trigger(parent TEXT)
```
Creates the trigger on UPDATE for HASH partitions. The UPDATE trigger isn't created by default because of overhead. It is useful in cases when key attribute could be changed.
```
create_range_update_trigger(parent TEXT)
```
Same as above for RANGE partitioned table.

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
append_range_partition(p_relation TEXT)
```
Appends new RANGE partition and returns 
```
prepend_range_partition(p_relation TEXT)
```
Prepends new RANGE partition.

```
add_range_partition(
    relation TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT)
```
Creates new RANGE partition for `relation` with specified values range.

```
drop_range_partition(partition TEXT)
```
Drops RANGE partition and all its data.

```
attach_range_partition(
    relation TEXT,
    partition TEXT,
    start_value ANYELEMENT,
    end_value ANYELEMENT)
```
Attaches partition to existing RANGE partitioned relation. The table being attached must have exact same structure as the parent one.

```
detach_range_partition(partition TEXT)
```
Detaches partition from existing RANGE partitioned relation.


```
disable_partitioning(relation TEXT)
```
Disables `pg_pathman` partitioning mechanism for the specified parent table and removes an insert trigger. Partitions itself remain unchanged.

## Examples
### HASH
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
If partitions are supposed to have indexes, then they should be created for parent table before partitioning. In this case pg_pathman will automaticaly create indexes for partitions. Then run create_hash_partitions() function with appropriate arguments:
```
SELECT create_hash_partitions('items', 'id', 100);
```
This will create new partitions and move the data from parent to partitions.
Here is an example of the query with filtering by partitioning key and its plan:
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
Note that pg_pathman exludes parent table from the query plan. To access parent table use ONLY modifier:
```
EXPLAIN SELECT * FROM ONLY items;
                      QUERY PLAN                      
------------------------------------------------------
 Seq Scan on items  (cost=0.00..0.00 rows=1 width=45)
```
### RANGE
Consider an example of RANGE partitioning. Let's create a table to store log data:
```
CREATE TABLE journal (
    id      SERIAL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
);
CREATE INDEX ON journal(dt);

INSERT INTO journal (dt, level, msg)
SELECT g, random()*6, md5(g::text)
FROM generate_series('2015-01-01'::date, '2015-12-31'::date, '1 minute') as g;
```
Run create_range_partitions() function to create partitions so that each partition would contain data for one day:
```
SELECT create_range_partitions('journal', 'dt', '2015-01-01'::date, '1 day'::interval);
```
It will create 365 partitions and move the data from parent to partitions.

New partitions are appended automaticaly by insert trigger. But it can be done manually with the following functions:
```
SELECT add_range_partition('journal', '2016-01-01'::date, '2016-01-07'::date);
SELECT append_range_partition('journal');
```
The first one creates partition with specified range. The second one creates partition with default interval and appends it to the partition list. It is also possible to attach an existing table as partition. For example we may want to attach an archive table (or even foreign table from another server) for outdated data:

```
CREATE FOREIGN TABLE journal_archive (
    id      INTEGER NOT NULL,
    dt      TIMESTAMP NOT NULL,
    level   INTEGER,
    msg     TEXT
) SERVER archive_server;

SELECT attach_range_partition('journal', 'journal_archive', '2014-01-01'::date, '2015-01-01'::date);
```
> Important: the structure of the table being attched must exactly match the parent.

To merge to adjacent partitions use function:
```
SELECT merge_range_partitions('journal_archive', 'journal_1');
```
To split partition by value use function:
```
SELECT split_range_partition('journal_366', '2016-01-03'::date);
```
To detach partition use:
```
SELECT detach_range_partition('journal_archive');
```

Here is an example of the query with filtering by partitioning key and its plan:
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

### Disable pg_pathman
To disable pg_pathman for some previously partitioned table use disable_partitioning() function:
```
SELECT disable_partitioning('range_rel');
```
All sections and data will stay available and will be handled by standard PostgreSQL partitioning mechanism.

## Authors

Ildar Musin <i.musin@postgrespro.ru> Postgres Professional Ltd., Russia

Alexander Korotkov <a.korotkov@postgrespro.ru> Postgres Professional Ltd., Russia
