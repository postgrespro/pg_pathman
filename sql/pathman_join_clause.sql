/*
 * Since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output; pathman_gaps_1.out is the updated version.
 */
\set VERBOSITY terse
SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;



/*
 * Test push down a join clause into child nodes of append
 */

/* create test tables */
CREATE TABLE test.fk (
	id1 INT NOT NULL,
	id2 INT NOT NULL,
	start_key INT,
	end_key INT,
	PRIMARY KEY (id1, id2));

CREATE TABLE test.mytbl (
	id1 INT NOT NULL,
	id2 INT NOT NULL,
	key INT NOT NULL,
	CONSTRAINT fk_fk FOREIGN KEY (id1, id2) REFERENCES test.fk(id1, id2),
	PRIMARY KEY (id1, key));

SELECT pathman.create_hash_partitions('test.mytbl', 'id1', 8);

/* ...fill out with test data */
INSERT INTO test.fk VALUES (1, 1);
INSERT INTO test.mytbl VALUES (1, 1, 5), (1, 1, 6);

/* gather statistics on test tables to have deterministic plans */
ANALYZE;


/* run test queries */
EXPLAIN (COSTS OFF)     /* test plan */
SELECT m.tableoid::regclass, id1, id2, key, start_key, end_key
FROM test.mytbl m JOIN test.fk USING(id1, id2)
WHERE NOT key <@ int4range(6, end_key);

/* test joint data */
SELECT m.tableoid::regclass, id1, id2, key, start_key, end_key
FROM test.mytbl m JOIN test.fk USING(id1, id2)
WHERE NOT key <@ int4range(6, end_key);



/*
 * Test case by @dimarick
 */

CREATE TABLE test.parent (
  id SERIAL NOT NULL,
  owner_id INTEGER NOT NULL
);

CREATE TABLE test.child (
  parent_id INTEGER NOT NULL,
  owner_id INTEGER NOT NULL
);

CREATE TABLE test.child_nopart (
  parent_id INTEGER NOT NULL,
  owner_id INTEGER NOT NULL
);

INSERT INTO test.parent (owner_id) VALUES (1), (2), (3), (3);
INSERT INTO test.child (parent_id, owner_id) VALUES (1, 1), (2, 2), (3, 3), (5, 3);
INSERT INTO test.child_nopart (parent_id, owner_id) VALUES (1, 1), (2, 2), (3, 3), (5, 3);

SELECT pathman.create_hash_partitions('test.child', 'owner_id', 2);

/* gather statistics on test tables to have deterministic plans */
ANALYZE;


/* Query #1 */
EXPLAIN (COSTS OFF) SELECT * FROM test.parent
LEFT JOIN test.child ON test.child.parent_id = test.parent.id AND
						test.child.owner_id = test.parent.owner_id
WHERE test.parent.owner_id = 3 and test.parent.id IN (3, 4);

SELECT * FROM test.parent
LEFT JOIN test.child ON test.child.parent_id = test.parent.id AND
						test.child.owner_id = test.parent.owner_id
WHERE test.parent.owner_id = 3 and test.parent.id IN (3, 4);


/* Query #2 */
EXPLAIN (COSTS OFF) SELECT * FROM test.parent
LEFT JOIN test.child ON test.child.parent_id = test.parent.id AND
						test.child.owner_id = 3
WHERE test.parent.owner_id = 3 and test.parent.id IN (3, 4);

SELECT * FROM test.parent
LEFT JOIN test.child ON test.child.parent_id = test.parent.id AND
						test.child.owner_id = 3
WHERE test.parent.owner_id = 3 and test.parent.id IN (3, 4);



DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
