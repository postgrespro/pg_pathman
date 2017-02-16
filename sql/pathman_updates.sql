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


/* tuple descs are the same */
EXPLAIN (COSTS OFF) UPDATE test_updates.test SET b = 0 WHERE val = 1;
UPDATE test_updates.test SET b = 0 WHERE val = 1 RETURNING *, tableoid::REGCLASS;


/* tuple descs are different */
EXPLAIN (COSTS OFF) UPDATE test_updates.test SET b = 0 WHERE val = 101;
UPDATE test_updates.test SET b = 0 WHERE val = 101 RETURNING *, tableoid::REGCLASS;



DROP SCHEMA test_updates CASCADE;
DROP EXTENSION pg_pathman;
