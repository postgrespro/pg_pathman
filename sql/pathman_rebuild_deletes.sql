/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on < 11 because planner now turns
 *  Row(Const, Const) into just Const of record type, apparently since 3decd150
 * -------------------------------------------
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_deletes;


/*
 * Test DELETEs on a partition with different TupleDescriptor.
 */

/* create partitioned table */
CREATE TABLE test_deletes.test(a FLOAT4, val INT4 NOT NULL, b FLOAT8);
INSERT INTO test_deletes.test SELECT i, i, i FROM generate_series(1, 100) AS i;
SELECT create_range_partitions('test_deletes.test', 'val', 1, 10);

/* drop column 'a' */
ALTER TABLE test_deletes.test DROP COLUMN a;

/* append new partition */
SELECT append_range_partition('test_deletes.test');
INSERT INTO test_deletes.test_11 (val, b) VALUES (101, 10);


VACUUM ANALYZE;


/* tuple descs are the same */
EXPLAIN (COSTS OFF) DELETE FROM test_deletes.test WHERE val = 1;
DELETE FROM test_deletes.test WHERE val = 1 RETURNING *, tableoid::REGCLASS;


/* tuple descs are different */
EXPLAIN (COSTS OFF) DELETE FROM test_deletes.test WHERE val = 101;
DELETE FROM test_deletes.test WHERE val = 101 RETURNING *, tableoid::REGCLASS;

CREATE TABLE test_deletes.test_dummy (val INT4);

EXPLAIN (COSTS OFF) DELETE FROM test_deletes.test
WHERE val = 101 AND val = ANY (TABLE test_deletes.test_dummy)
RETURNING *, tableoid::REGCLASS;

EXPLAIN (COSTS OFF) DELETE FROM test_deletes.test t1
USING test_deletes.test_dummy t2
WHERE t1.val = 101 AND t1.val = t2.val
RETURNING t1.*, t1.tableoid::REGCLASS;

EXPLAIN (COSTS OFF) DELETE FROM test_deletes.test
WHERE val = 101 AND test >= (100, 8)
RETURNING *, tableoid::REGCLASS;

DROP TABLE test_deletes.test_dummy;



DROP TABLE test_deletes.test CASCADE;
DROP SCHEMA test_deletes;
DROP EXTENSION pg_pathman;
