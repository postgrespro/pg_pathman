\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

SET enable_indexscan = ON;
SET enable_seqscan = OFF;

/* Temporary table for JOINs */
CREATE TABLE test.tmp (id INTEGER NOT NULL, value INTEGER NOT NULL);
INSERT INTO test.tmp VALUES (1, 1), (2, 2);

/* Range */
CREATE TABLE test.range_rel (
	id		SERIAL PRIMARY KEY,
	dt		TIMESTAMP NOT NULL,
	value	INTEGER);
INSERT INTO test.range_rel (dt, value) SELECT g, extract(day from g) FROM generate_series('2010-01-01'::date, '2010-12-31'::date, '1 day') as g;
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 12);

/* Test UPDATE and DELETE */
EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';	/* have partitions for this 'dt' */
UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt = '2010-06-15';	/* have partitions for this 'dt' */
DELETE FROM test.range_rel WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';

EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 222 WHERE dt = '1990-01-01';	/* no partitions for this 'dt' */
UPDATE test.range_rel SET value = 111 WHERE dt = '1990-01-01';
SELECT * FROM test.range_rel WHERE dt = '1990-01-01';

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt < '1990-01-01';	/* no partitions for this 'dt' */
DELETE FROM test.range_rel WHERE dt < '1990-01-01';
SELECT * FROM test.range_rel WHERE dt < '1990-01-01';

EXPLAIN (COSTS OFF) UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;
DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;

EXPLAIN (COSTS OFF) DELETE FROM test.tmp t USING test.range_rel r WHERE r.dt = '2010-01-02' AND r.id = t.id;
DELETE FROM test.tmp t USING test.range_rel r WHERE r.dt = '2010-01-02' AND r.id = t.id;

DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
