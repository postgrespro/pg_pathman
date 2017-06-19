CREATE EXTENSION pg_pathman;
CREATE SCHEMA rowmarks;



CREATE TABLE rowmarks.first(id int NOT NULL);
CREATE TABLE rowmarks.second(id int NOT NULL);

INSERT INTO rowmarks.first SELECT generate_series(1, 10);
INSERT INTO rowmarks.second SELECT generate_series(1, 10);


SELECT create_hash_partitions('rowmarks.first', 'id', 5);

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

/* Check updates (plan) */
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

/* Check updates (execution) */
UPDATE rowmarks.second SET id = 1
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1 OR id = 2)
RETURNING *, tableoid::regclass;

/* Check deletes (plan) */
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1);
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id < 1);
EXPLAIN (COSTS OFF)
DELETE FROM rowmarks.second
WHERE rowmarks.second.id IN (SELECT id FROM rowmarks.first WHERE id = 1 OR id = 2);



DROP SCHEMA rowmarks CASCADE;
DROP EXTENSION pg_pathman;
