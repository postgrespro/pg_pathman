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



DROP SCHEMA test_updates CASCADE;
DROP EXTENSION pg_pathman;
