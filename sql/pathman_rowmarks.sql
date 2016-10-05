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


DROP SCHEMA rowmarks CASCADE;
DROP EXTENSION pg_pathman;
