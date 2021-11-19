\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_update_node;


SET pg_pathman.enable_partitionrouter = ON;


/* Partition table by RANGE (NUMERIC) */
CREATE TABLE test_update_node.test_range(val NUMERIC NOT NULL, comment TEXT);
CREATE INDEX val_idx ON test_update_node.test_range (val);
INSERT INTO test_update_node.test_range SELECT i, i FROM generate_series(1, 100) i;
SELECT create_range_partitions('test_update_node.test_range', 'val', 1, 10);

/* Moving from 2st to 1st partition */
EXPLAIN (COSTS OFF) UPDATE test_update_node.test_range SET val = 5 WHERE val = 15;

/* Keep same partition */
EXPLAIN (COSTS OFF) UPDATE test_update_node.test_range SET val = 14 WHERE val = 15;

/* Update values in 1st partition (rows remain there) */
UPDATE test_update_node.test_range SET val = 5 WHERE val <= 10;

/* Check values #1 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val < 10
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Update values in 2nd partition (rows move to 3rd partition) */
UPDATE test_update_node.test_range SET val = val + 10 WHERE val > 10 AND val <= 20;

/* Check values #2 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val > 20 AND val <= 30
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Move single row */
UPDATE test_update_node.test_range SET val = 90 WHERE val = 80;

/* Check values #3 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 90
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Move single row (create new partition) */
UPDATE test_update_node.test_range SET val = -1 WHERE val = 50;

/* Check values #4 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = -1
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Update non-key column */
UPDATE test_update_node.test_range SET comment = 'test!' WHERE val = 100;

/* Check values #5 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 100
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Try moving row into a gap (ERROR) */
DROP TABLE test_update_node.test_range_4;
UPDATE test_update_node.test_range SET val = 35 WHERE val = 70;

/* Check values #6 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 70
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Test trivial move (same key) */
UPDATE test_update_node.test_range SET val = 65 WHERE val = 65;

/* Check values #7 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 65
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_range;


/* Test tuple conversion (attached partition) */
CREATE TABLE test_update_node.test_range_inv(comment TEXT, val NUMERIC NOT NULL);
SELECT attach_range_partition('test_update_node.test_range',
							  'test_update_node.test_range_inv',
							  101::NUMERIC, 111::NUMERIC);
UPDATE test_update_node.test_range SET val = 105 WHERE val = 60;
UPDATE test_update_node.test_range SET val = 105 WHERE val = 105;

/* Check values #8 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 105
ORDER BY comment;

UPDATE test_update_node.test_range SET val = 60 WHERE val = 105;
SELECT count(*) FROM test_update_node.test_range;

/* Test RETURNING */
UPDATE test_update_node.test_range SET val = 71 WHERE val = 41 RETURNING val, comment;
UPDATE test_update_node.test_range SET val = 71 WHERE val = 71 RETURNING val, comment;
UPDATE test_update_node.test_range SET val = 106 WHERE val = 61 RETURNING val, comment;
UPDATE test_update_node.test_range SET val = 106 WHERE val = 106 RETURNING val, comment;
UPDATE test_update_node.test_range SET val = 61 WHERE val = 106 RETURNING val, comment;

/* Just in case, check we don't duplicate anything */
SELECT count(*) FROM test_update_node.test_range;

/* Test tuple conversion (dropped column) */
ALTER TABLE test_update_node.test_range DROP COLUMN comment CASCADE;
SELECT append_range_partition('test_update_node.test_range');
UPDATE test_update_node.test_range SET val = 115 WHERE val = 55;
UPDATE test_update_node.test_range SET val = 115 WHERE val = 115;

/* Check values #9 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_range
WHERE val = 115;

UPDATE test_update_node.test_range SET val = 55 WHERE val = 115;
SELECT count(*) FROM test_update_node.test_range;

DROP TABLE test_update_node.test_range CASCADE;

/* recreate table and mass move */
CREATE TABLE test_update_node.test_range(val NUMERIC NOT NULL, comment TEXT);
INSERT INTO test_update_node.test_range SELECT i, i FROM generate_series(1, 100) i;
SELECT create_range_partitions('test_update_node.test_range', 'val', 1, 10);

SELECT tableoid::regclass, MIN(val) FROM test_update_node.test_range
	GROUP BY tableoid::regclass ORDER BY tableoid::regclass;
SELECT count(*) FROM test_update_node.test_range;

/* move everything to next partition */
UPDATE test_update_node.test_range SET val = val + 10;
SELECT tableoid::regclass, MIN(val) FROM test_update_node.test_range
	GROUP BY tableoid::regclass ORDER BY tableoid::regclass;

/* move everything to previous partition */
UPDATE test_update_node.test_range SET val = val - 10;
SELECT tableoid::regclass, MIN(val) FROM test_update_node.test_range
	GROUP BY tableoid::regclass ORDER BY tableoid::regclass;
SELECT count(*) FROM test_update_node.test_range;

/* Partition table by HASH (INT4) */
CREATE TABLE test_update_node.test_hash(val INT4 NOT NULL, comment TEXT);
INSERT INTO test_update_node.test_hash SELECT i, i FROM generate_series(1, 10) i;
SELECT create_hash_partitions('test_update_node.test_hash', 'val', 3);


/* Shuffle rows a few times */
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;
UPDATE test_update_node.test_hash SET val = val + 1;

/* Check values #0 */
SELECT tableoid::regclass, * FROM test_update_node.test_hash ORDER BY val;


/* Move all rows into single partition */
UPDATE test_update_node.test_hash SET val = 1;

/* Check values #1 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_hash
WHERE val = 1
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_hash;


/* Don't move any rows */
UPDATE test_update_node.test_hash SET val = 3 WHERE val = 2;

/* Check values #2 */
SELECT tableoid::REGCLASS, *
FROM test_update_node.test_hash
WHERE val = 3
ORDER BY comment;

SELECT count(*) FROM test_update_node.test_hash;



DROP TABLE test_update_node.test_hash CASCADE;
DROP TABLE test_update_node.test_range CASCADE;
DROP SCHEMA test_update_node;
DROP EXTENSION pg_pathman;
