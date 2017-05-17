\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_update_trigger;



/* Partition table by RANGE (NUMERIC) */
CREATE TABLE test_update_trigger.test_range(val NUMERIC NOT NULL, comment TEXT);
INSERT INTO test_update_trigger.test_range SELECT i, i FROM generate_series(1, 100) i;
SELECT create_range_partitions('test_update_trigger.test_range', 'val', 1, 10);
SELECT create_update_triggers('test_update_trigger.test_range');


/* Update values in 1st partition (rows remain there) */
UPDATE test_update_trigger.test_range SET val = 5 WHERE val <= 10;

/* Check values #1 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val < 10
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Update values in 2nd partition (rows move to 3rd partition) */
UPDATE test_update_trigger.test_range SET val = val + 10 WHERE val > 10 AND val <= 20;

/* Check values #2 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val > 20 AND val <= 30
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Move single row */
UPDATE test_update_trigger.test_range SET val = 90 WHERE val = 80;

/* Check values #3 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 90
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Move single row (create new partition) */
UPDATE test_update_trigger.test_range SET val = -1 WHERE val = 50;

/* Check values #4 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = -1
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Update non-key column */
UPDATE test_update_trigger.test_range SET comment = 'test!' WHERE val = 100;

/* Check values #5 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 100
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Try moving row into a gap (ERROR) */
DROP TABLE test_update_trigger.test_range_4;
UPDATE test_update_trigger.test_range SET val = 35 WHERE val = 70;

/* Check values #6 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 70
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Test trivial move (same key) */
UPDATE test_update_trigger.test_range SET val = 65 WHERE val = 65;

/* Check values #7 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 65
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Test tuple conversion (attached partition) */
CREATE TABLE test_update_trigger.test_range_inv(comment TEXT, val NUMERIC NOT NULL);
SELECT attach_range_partition('test_update_trigger.test_range',
							  'test_update_trigger.test_range_inv',
							  101::NUMERIC, 111::NUMERIC);
UPDATE test_update_trigger.test_range SET val = 105 WHERE val = 60;

/* Check values #8 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 105
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_range;


/* Test tuple conversion (dropped column) */
ALTER TABLE test_update_trigger.test_range DROP COLUMN comment CASCADE;
SELECT append_range_partition('test_update_trigger.test_range');
UPDATE test_update_trigger.test_range SET val = 115 WHERE val = 55;

/* Check values #9 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_range
WHERE val = 115;

SELECT count(*) FROM test_update_trigger.test_range;



/* Partition table by HASH (INT4) */
CREATE TABLE test_update_trigger.test_hash(val INT4 NOT NULL, comment TEXT);
INSERT INTO test_update_trigger.test_hash SELECT i, i FROM generate_series(1, 10) i;
SELECT create_hash_partitions('test_update_trigger.test_hash', 'val', 3);
SELECT create_update_triggers('test_update_trigger.test_hash');


/* Move all rows into single partition */
UPDATE test_update_trigger.test_hash SET val = 1;

/* Check values #1 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_hash
WHERE val = 1
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_hash;


/* Don't move any rows */
UPDATE test_update_trigger.test_hash SET val = 3 WHERE val = 2;

/* Check values #2 */
SELECT tableoid::REGCLASS, *
FROM test_update_trigger.test_hash
WHERE val = 3
ORDER BY comment;

SELECT count(*) FROM test_update_trigger.test_hash;



DROP SCHEMA test_update_trigger CASCADE;
DROP EXTENSION pg_pathman;
