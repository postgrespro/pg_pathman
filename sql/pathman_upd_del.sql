/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on 9.5
 * -------------------------------------------
 */

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

CREATE TABLE test.tmp2 (id INTEGER NOT NULL, value INTEGER NOT NULL);
SELECT pathman.create_range_partitions('test.tmp2', 'id', 1, 1, 10);


/* Partition table by RANGE */
CREATE TABLE test.range_rel (
	id		SERIAL PRIMARY KEY,
	dt		TIMESTAMP NOT NULL,
	value	INTEGER);

INSERT INTO test.range_rel (dt, value) SELECT g, extract(day from g)
FROM generate_series('2010-01-01'::date, '2010-12-31'::date, '1 day') AS g;

SELECT pathman.create_range_partitions('test.range_rel', 'dt',
									   '2010-01-01'::date, '1 month'::interval,
									   12);


/*
 * Test UPDATE and DELETE
 */

/* have partitions for this 'dt' */
EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';

BEGIN;
UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';
ROLLBACK;


/* have partitions for this 'dt' */
EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt = '2010-06-15';

BEGIN;
DELETE FROM test.range_rel WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';
ROLLBACK;


/* no partitions for this 'dt' */
EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 222 WHERE dt = '1990-01-01';

BEGIN;
UPDATE test.range_rel SET value = 111 WHERE dt = '1990-01-01';
SELECT * FROM test.range_rel WHERE dt = '1990-01-01';
ROLLBACK;


/* no partitions for this 'dt' */
EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt < '1990-01-01';

BEGIN;
DELETE FROM test.range_rel WHERE dt < '1990-01-01';
SELECT * FROM test.range_rel WHERE dt < '1990-01-01';
ROLLBACK;


/* UPDATE + FROM, partitioned table */
EXPLAIN (COSTS OFF)
UPDATE test.range_rel r SET value = t.value
FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;

BEGIN;
UPDATE test.range_rel r SET value = t.value
FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
ROLLBACK;


/* UPDATE + FROM, single table */
EXPLAIN (COSTS OFF)
UPDATE test.tmp t SET value = r.value
FROM test.range_rel r WHERE r.dt = '2010-01-01' AND r.id = t.id;

BEGIN;
UPDATE test.tmp t SET value = r.value
FROM test.range_rel r WHERE r.dt = '2010-01-01' AND r.id = t.id;
ROLLBACK;


/* DELETE + USING, partitioned table */
EXPLAIN (COSTS OFF)
DELETE FROM test.range_rel r USING test.tmp t
WHERE r.dt = '2010-01-02' AND r.id = t.id;

BEGIN;
DELETE FROM test.range_rel r USING test.tmp t
WHERE r.dt = '2010-01-02' AND r.id = t.id;
ROLLBACK;


/* DELETE + USING, single table */
EXPLAIN (COSTS OFF)
DELETE FROM test.tmp t USING test.range_rel r
WHERE r.dt = '2010-01-02' AND r.id = t.id;

BEGIN;
DELETE FROM test.tmp t USING test.range_rel r
WHERE r.dt = '2010-01-02' AND r.id = t.id;
ROLLBACK;


/* Test special rule for CTE; SELECT (PostgreSQL 9.5) */
EXPLAIN (COSTS OFF)
WITH q AS (SELECT * FROM test.range_rel r
		   WHERE r.dt = '2010-01-02')
DELETE FROM test.tmp USING q;

BEGIN;
WITH q AS (SELECT * FROM test.range_rel r
		   WHERE r.dt = '2010-01-02')
DELETE FROM test.tmp USING q;
ROLLBACK;


/* Test special rule for CTE; DELETE (PostgreSQL 9.5) */
EXPLAIN (COSTS OFF)
WITH q AS (DELETE FROM test.range_rel r
		   WHERE r.dt = '2010-01-02'
		   RETURNING *)
DELETE FROM test.tmp USING q;

BEGIN;
WITH q AS (DELETE FROM test.range_rel r
		   WHERE r.dt = '2010-01-02'
		   RETURNING *)
DELETE FROM test.tmp USING q;
ROLLBACK;


/* Test special rule for CTE; DELETE + USING (PostgreSQL 9.5) */
EXPLAIN (COSTS OFF)
WITH q AS (DELETE FROM test.tmp t
		   USING test.range_rel r
		   WHERE r.dt = '2010-01-02' AND r.id = t.id
		   RETURNING *)
DELETE FROM test.tmp USING q;

BEGIN;
WITH q AS (DELETE FROM test.tmp t
		   USING test.range_rel r
		   WHERE r.dt = '2010-01-02' AND r.id = t.id
		   RETURNING *)
DELETE FROM test.tmp USING q;
ROLLBACK;

/* Test special rule for CTE; DELETE + USING with partitioned table */
DELETE FROM test.range_rel r USING test.tmp2 t WHERE t.id = r.id;

DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
