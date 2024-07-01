/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on < 11 because planner now turns
 *  Row(Const, Const) into just Const of record type, apparently since 3decd150
 * -------------------------------------------
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_updates;


/*
 * Test UPDATEs on a partition with different TupleDescriptor.
 */

/* create partitioned table */
CREATE TABLE test_updates.test(a FLOAT4, val INT4 NOT NULL, b FLOAT8);
INSERT INTO test_updates.test SELECT i, i, i FROM generate_series(1, 100) AS i;
SELECT create_range_partitions('test_updates.test', 'val', 1, 10);

/* drop column 'a' */
ALTER TABLE test_updates.test DROP COLUMN a;

/* append new partition */
SELECT append_range_partition('test_updates.test');
INSERT INTO test_updates.test_11 (val, b) VALUES (101, 10);


VACUUM ANALYZE;


/* tuple descs are the same */
EXPLAIN (COSTS OFF) UPDATE test_updates.test SET b = 0 WHERE val = 1;
UPDATE test_updates.test SET b = 0 WHERE val = 1 RETURNING *, tableoid::REGCLASS;


/* tuple descs are different */
EXPLAIN (COSTS OFF) UPDATE test_updates.test SET b = 0 WHERE val = 101;
UPDATE test_updates.test SET b = 0 WHERE val = 101 RETURNING *, tableoid::REGCLASS;

CREATE TABLE test_updates.test_dummy (val INT4);

EXPLAIN (COSTS OFF) UPDATE test_updates.test SET val = val + 1
WHERE val = 101 AND val = ANY (TABLE test_updates.test_dummy)
RETURNING *, tableoid::REGCLASS;

EXPLAIN (COSTS OFF) UPDATE test_updates.test t1 SET b = 0
FROM test_updates.test_dummy t2
WHERE t1.val = 101 AND t1.val = t2.val
RETURNING t1.*, t1.tableoid::REGCLASS;

EXPLAIN (COSTS OFF) UPDATE test_updates.test SET b = 0
WHERE val = 101 AND test >= (100, 8)
RETURNING *, tableoid::REGCLASS;

/* execute this one */
UPDATE test_updates.test SET b = 0
WHERE val = 101 AND test >= (100, -1)
RETURNING test;

DROP TABLE test_updates.test_dummy;


/* cross-partition updates (& different tuple descs) */
TRUNCATE test_updates.test;
SET pg_pathman.enable_partitionrouter = ON;

SELECT *, (select count(*) from pg_attribute where attrelid = partition) as columns
FROM pathman_partition_list
ORDER BY range_min::int, range_max::int;

INSERT INTO test_updates.test VALUES (105, 105);
UPDATE test_updates.test SET val = 106 WHERE val = 105 RETURNING *, tableoid::REGCLASS;
UPDATE test_updates.test SET val = 115 WHERE val = 106 RETURNING *, tableoid::REGCLASS;
UPDATE test_updates.test SET val =  95 WHERE val = 115 RETURNING *, tableoid::REGCLASS;
UPDATE test_updates.test SET val =  -1 WHERE val =  95 RETURNING *, tableoid::REGCLASS;


/* basic check for 'ALTER TABLE ... ADD COLUMN'; PGPRO-5113 */
create table test_updates.test_5113(val int4 not null);
insert into test_updates.test_5113 values (1);
select create_range_partitions('test_updates.test_5113', 'val', 1, 10);
update test_updates.test_5113 set val = 11 where val = 1;
alter table test_updates.test_5113 add column x varchar;
/* no error here: */
select * from test_updates.test_5113 where val = 11;
drop table test_updates.test_5113 cascade;

create table test_updates.test_5113(val int4 not null);
insert into test_updates.test_5113 values (1);
select create_range_partitions('test_updates.test_5113', 'val', 1, 10);
update test_updates.test_5113 set val = 11 where val = 1;
alter table test_updates.test_5113 add column x int8;
/* no extra data in column 'x' here: */
select * from test_updates.test_5113 where val = 11;
drop table test_updates.test_5113 cascade;


DROP TABLE test_updates.test CASCADE;
DROP SCHEMA test_updates;
DROP EXTENSION pg_pathman;
