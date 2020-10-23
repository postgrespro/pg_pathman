/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on 9.5
 * -------------------------------------------
 *
 * Also since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output; pathman_rowmarks_2.out is the updated version.
 *
 * Since 55a1954da16 and 6ef77cf46e8 (>= 13) output of EXPLAIN was changed,
 * now it includes aliases for inherited tables.
 */
SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA rowmarks;



CREATE TABLE rowmarks.first(id int NOT NULL);
CREATE TABLE rowmarks.second(id int NOT NULL);

INSERT INTO rowmarks.first SELECT generate_series(1, 10);
INSERT INTO rowmarks.second SELECT generate_series(1, 10);


SELECT create_hash_partitions('rowmarks.first', 'id', 5);


VACUUM ANALYZE;


/* Not partitioned */
SELECT * FROM rowmarks.second ORDER BY id FOR UPDATE;

/* Simple case (plan) */
EXPLAIN (COSTS OFF)
SELECT * FROM rowmarks.first ORDER BY id FOR UPDATE;

/* Simple case (execution) */
SELECT * FROM rowmarks.first ORDER BY id FOR UPDATE;
SELECT FROM rowmarks.first ORDER BY id FOR UPDATE;
SELECT tableoid > 0 FROM rowmarks.first ORDER BY id FOR UPDATE;

/* A little harder (plan) */
EXPLAIN (COSTS OFF)
SELECT * FROM rowmarks.first
WHERE id = (SELECT id FROM rowmarks.first
			ORDER BY id
			OFFSET 10 LIMIT 1
			FOR UPDATE)
FOR SHARE;

/* A little harder (execution) */
SELECT * FROM rowmarks.first
WHERE id = (SELECT id FROM rowmarks.first
			ORDER BY id
			OFFSET 5 LIMIT 1
			FOR UPDATE)
FOR SHARE;

/* Two tables (plan) */
EXPLAIN (COSTS OFF)
SELECT * FROM rowmarks.first
WHERE id = (SELECT id FROM rowmarks.second
			ORDER BY id
			OFFSET 5 LIMIT 1
			FOR UPDATE)
FOR SHARE;

/* Two tables (execution) */
SELECT * FROM rowmarks.first
WHERE id = (SELECT id FROM rowmarks.second
			ORDER BY id
			OFFSET 5 LIMIT 1
			FOR UPDATE)
FOR SHARE;

/* JOIN (plan) */
EXPLAIN (COSTS OFF)
SELECT * FROM rowmarks.first
JOIN rowmarks.second USING(id)
ORDER BY id
FOR UPDATE;

/* JOIN (execution) */
SELECT * FROM rowmarks.first
JOIN rowmarks.second USING(id)
ORDER BY id
FOR UPDATE;

/* ONLY (plan) */
EXPLAIN (COSTS OFF)
SELECT * FROM ONLY rowmarks.first FOR SHARE;

/* ONLY (execution) */
SELECT * FROM ONLY rowmarks.first FOR SHARE;

/* Check updates (plan) */
SET enable_hashjoin = f;	/* Hash Semi Join on 10 vs Hash Join on 9.6 */
SET enable_mergejoin = f;	/* Merge Semi Join on 10 vs Merge Join on 9.6 */
EXPLAIN (COSTS OFF)
UPDATE rowmarks.second SET id = 2
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1);
EXPLAIN (COSTS OFF)
UPDATE rowmarks.second SET id = 2
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id < 1);
EXPLAIN (COSTS OFF)
UPDATE rowmarks.second SET id = 2
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1 OR id = 2);
EXPLAIN (COSTS OFF)
UPDATE rowmarks.second SET id = 2
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1)
RETURNING *, tableoid::regclass;
SET enable_hashjoin = t;
SET enable_mergejoin = t;

/* Check updates (execution) */
UPDATE rowmarks.second SET id = 1
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1 OR id = 2)
RETURNING *, tableoid::regclass;

/* Check deletes (plan) */
SET enable_hashjoin = f;	/* Hash Semi Join on 10 vs Hash Join on 9.6 */
SET enable_mergejoin = f;	/* Merge Semi Join on 10 vs Merge Join on 9.6 */
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1);
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id < 1);
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1 OR id = 2);
SET enable_hashjoin = t;
SET enable_mergejoin = t;



DROP SCHEMA rowmarks CASCADE;
DROP EXTENSION pg_pathman;
