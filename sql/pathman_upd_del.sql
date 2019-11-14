/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on 9.5
 * -------------------------------------------
 *
 * Also since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output. Moreover, again since 12 (608b167f9f), CTEs which are
 * scanned once are no longer an optimization fence, changing a good deal of
 * plans here. There is an option to forcibly make them MATERIALIZED, but we
 * also need to run tests on older versions, so put updated plans in
 * pathman_upd_del_2.out instead.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;



SET enable_indexscan = ON;
SET enable_seqscan = OFF;


/* Temporary tables for JOINs */
CREATE TABLE test.tmp (id INTEGER NOT NULL, value INTEGER NOT NULL);
INSERT INTO test.tmp VALUES (1, 1), (2, 2);

CREATE TABLE test.tmp2 (id INTEGER NOT NULL, value INTEGER NOT NULL);
INSERT INTO test.tmp2 SELECT i % 10 + 1, i FROM generate_series(1, 100) i;
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


VACUUM ANALYZE;


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


/* DELETE + USING, two partitioned tables */
EXPLAIN (COSTS OFF)
DELETE FROM test.range_rel r USING test.tmp2 t
WHERE t.id = r.id;

BEGIN;
DELETE FROM test.range_rel r USING test.tmp2 t
WHERE t.id = r.id;
ROLLBACK;


/* DELETE + USING, partitioned table + two partitioned tables in subselect */
EXPLAIN (COSTS OFF)
DELETE FROM test.range_rel r
USING (SELECT *
	   FROM test.tmp2 a1
	   JOIN test.tmp2 a2
	   USING(id)) t
WHERE t.id = r.id;

BEGIN;
DELETE FROM test.range_rel r
USING (SELECT *
	   FROM test.tmp2 a1
	   JOIN test.tmp2 a2
	   USING(id)) t
WHERE t.id = r.id;
ROLLBACK;


/* DELETE + USING, single table + two partitioned tables in subselect */
EXPLAIN (COSTS OFF)
DELETE FROM test.tmp r
USING (SELECT *
	   FROM test.tmp2 a1
	   JOIN test.tmp2 a2
	   USING(id)) t
WHERE t.id = r.id;

BEGIN;
DELETE FROM test.tmp r
USING (SELECT *
	   FROM test.tmp2 a1
	   JOIN test.tmp2 a2
	   USING(id)) t
WHERE t.id = r.id;
ROLLBACK;


/* UPDATE + FROM, two partitioned tables */
EXPLAIN (COSTS OFF)
UPDATE test.range_rel r SET value = 1 FROM test.tmp2 t
WHERE t.id = r.id;

BEGIN;
UPDATE test.range_rel r SET value = 1 FROM test.tmp2 t
WHERE t.id = r.id;
ROLLBACK;


/*
 * UPDATE + subquery with partitioned table (PG 9.5).
 * See pathman_rel_pathlist_hook() + RELOPT_OTHER_MEMBER_REL.
 */
EXPLAIN (COSTS OFF)
UPDATE test.tmp t SET value = 2
WHERE t.id IN (SELECT id
			   FROM test.tmp2 t2
			   WHERE id = t.id);


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


/* Test special rule for CTE; Nested CTEs (PostgreSQL 9.5) */
EXPLAIN (COSTS OFF)
WITH q AS (WITH n AS (SELECT id FROM test.tmp2 WHERE id = 2)
		   DELETE FROM test.tmp t
		   USING n
		   WHERE t.id = n.id
		   RETURNING *)
DELETE FROM test.tmp USING q;


/* Test special rule for CTE; CTE in quals (PostgreSQL 9.5) */
EXPLAIN (COSTS OFF)
WITH q AS (SELECT id FROM test.tmp2
		   WHERE id < 3)
DELETE FROM test.tmp t WHERE t.id in (SELECT id FROM q);

BEGIN;
WITH q AS (SELECT id FROM test.tmp2
		   WHERE id < 3)
DELETE FROM test.tmp t WHERE t.id in (SELECT id FROM q);
ROLLBACK;



DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
