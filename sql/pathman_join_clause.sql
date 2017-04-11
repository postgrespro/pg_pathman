\set VERBOSITY terse

CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;


/*
 * Test push down a join clause into child nodes of append
 */

/* create test tables */
CREATE TABLE fk (
    id1 INT NOT NULL,
    id2 INT NOT NULL,
    start_key INT,
    end_key INT,
    PRIMARY KEY (id1, id2));
CREATE TABLE mytbl (
    id1 INT NOT NULL,
    id2 INT NOT NULL,
    key INT NOT NULL,
    CONSTRAINT fk_fk FOREIGN KEY (id1, id2) REFERENCES fk(id1, id2),
    PRIMARY KEY (id1, key));
SELECT pathman.create_hash_partitions('mytbl', 'id1', 8);

/* ...fill out with test data */
INSERT INTO fk VALUES (1, 1);
INSERT INTO mytbl VALUES (1, 1, 5), (1,1,6);

/* gather statistics on test tables to have deterministic plans */
ANALYZE fk;
ANALYZE mytbl;

/* run test queries */
EXPLAIN (COSTS OFF)     /* test plan */
SELECT m.tableoid::regclass, id1, id2, key, start_key, end_key
    FROM mytbl m JOIN fk USING(id1, id2)
    WHERE NOT key <@ int4range(6, end_key);
/* test joint data */
SELECT m.tableoid::regclass, id1, id2, key, start_key, end_key
    FROM mytbl m JOIN fk USING(id1, id2)
    WHERE NOT key <@ int4range(6, end_key);


DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;

