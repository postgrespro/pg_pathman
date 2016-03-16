\set VERBOSITY terse

CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

CREATE TABLE test.hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER);
INSERT INTO test.hash_rel VALUES (1, 1);
INSERT INTO test.hash_rel VALUES (2, 2);
INSERT INTO test.hash_rel VALUES (3, 3);
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
ALTER TABLE test.hash_rel ALTER COLUMN value SET NOT NULL;
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
SELECT COUNT(*) FROM test.hash_rel;
SELECT COUNT(*) FROM ONLY test.hash_rel;
INSERT INTO test.hash_rel VALUES (4, 4);
INSERT INTO test.hash_rel VALUES (5, 5);
INSERT INTO test.hash_rel VALUES (6, 6);
SELECT COUNT(*) FROM test.hash_rel;
SELECT COUNT(*) FROM ONLY test.hash_rel;

CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP,
	txt	TEXT);
CREATE INDEX ON test.range_rel (dt);
INSERT INTO test.range_rel (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) as g;
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL, 2);
ALTER TABLE test.range_rel ALTER COLUMN dt SET NOT NULL;
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL, 2);
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL);
SELECT COUNT(*) FROM test.range_rel;
SELECT COUNT(*) FROM ONLY test.range_rel;

CREATE TABLE test.num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);
SELECT pathman.create_range_partitions('test.num_range_rel', 'id', 0, 1000, 4);
SELECT COUNT(*) FROM test.num_range_rel;
SELECT COUNT(*) FROM ONLY test.num_range_rel;
INSERT INTO test.num_range_rel
	SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;
SELECT COUNT(*) FROM test.num_range_rel;
SELECT COUNT(*) FROM ONLY test.num_range_rel;

VACUUM;

/* update triggers test */
SELECT pathman.create_hash_update_trigger('test.hash_rel');
UPDATE test.hash_rel SET value = 7 WHERE value = 6;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 7;
SELECT * FROM test.hash_rel WHERE value = 7;

SELECT pathman.create_range_update_trigger('test.num_range_rel');
UPDATE test.num_range_rel SET id = 3001 WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id = 3001;
SELECT * FROM test.num_range_rel WHERE id = 3001;

SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_seqscan = ON;

EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2 OR value = 1;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2015-02-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-15' AND dt < '2015-03-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE (dt >= '2015-01-15' AND dt < '2015-02-15') OR (dt > '2015-03-15');


SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;
SET enable_seqscan = OFF;

EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2 OR value = 1;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2015-02-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-15' AND dt < '2015-03-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE (dt >= '2015-01-15' AND dt < '2015-02-15') OR (dt > '2015-03-15');

/*
 * Test CTE query
 */
EXPLAIN (COSTS OFF)
    WITH ttt AS (SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-15')
SELECT * FROM ttt;

EXPLAIN (COSTS OFF)
    WITH ttt AS (SELECT * FROM test.hash_rel WHERE value = 2)
SELECT * FROM ttt;

/*
 * Test split and merge
 */

/* Split first partition in half */
SELECT pathman.split_range_partition('test.num_range_rel_1', 500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id BETWEEN 100 AND 700;

SELECT pathman.split_range_partition('test.range_rel_1', '2015-01-15'::DATE);

/* Merge two partitions into one */
SELECT pathman.merge_range_partitions('test.num_range_rel_1', 'test.num_range_rel_' || currval('test.num_range_rel_seq'));
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id BETWEEN 100 AND 700;

SELECT pathman.merge_range_partitions('test.range_rel_1', 'test.range_rel_' || currval('test.range_rel_seq'));

/* Append and prepend partitions */
SELECT pathman.append_range_partition('test.num_range_rel');
SELECT pathman.prepend_range_partition('test.num_range_rel');
SELECT pathman.drop_range_partition('test.num_range_rel_7');

SELECT pathman.append_range_partition('test.range_rel');
SELECT pathman.prepend_range_partition('test.range_rel');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt BETWEEN '2014-12-15' AND '2015-01-15';
SELECT pathman.drop_range_partition('test.range_rel_7');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt BETWEEN '2014-12-15' AND '2015-01-15';
SELECT pathman.add_range_partition('test.range_rel', '2014-12-01'::DATE, '2015-01-02'::DATE);
SELECT pathman.add_range_partition('test.range_rel', '2014-12-01'::DATE, '2015-01-01'::DATE);
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt BETWEEN '2014-12-15' AND '2015-01-15';
CREATE TABLE test.range_rel_archive (LIKE test.range_rel INCLUDING ALL);
SELECT pathman.attach_range_partition('test.range_rel', 'test.range_rel_archive', '2014-01-01'::DATE, '2015-01-01'::DATE);
SELECT pathman.attach_range_partition('test.range_rel', 'test.range_rel_archive', '2014-01-01'::DATE, '2014-12-01'::DATE);
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt BETWEEN '2014-11-15' AND '2015-01-15';
SELECT pathman.detach_range_partition('test.range_rel_archive');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt BETWEEN '2014-11-15' AND '2015-01-15';
CREATE TABLE test.range_rel_test1 (
    id  SERIAL PRIMARY KEY,
    dt  TIMESTAMP,
    txt TEXT,
    abc INTEGER);
SELECT pathman.attach_range_partition('test.range_rel', 'test.range_rel_test1', '2013-01-01'::DATE, '2014-01-01'::DATE);
CREATE TABLE test.range_rel_test2 (
    id  SERIAL PRIMARY KEY,
    dt  TIMESTAMP);
SELECT pathman.attach_range_partition('test.range_rel', 'test.range_rel_test2', '2013-01-01'::DATE, '2014-01-01'::DATE);

/*
 * Clean up
 */
SELECT pathman.drop_hash_partitions('test.hash_rel');
SELECT COUNT(*) FROM ONLY test.hash_rel;
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
SELECT pathman.drop_hash_partitions('test.hash_rel', TRUE);
SELECT COUNT(*) FROM ONLY test.hash_rel;
DROP TABLE test.hash_rel CASCADE;

SELECT pathman.drop_range_partitions('test.num_range_rel');
DROP TABLE test.num_range_rel CASCADE;

DROP TABLE test.range_rel CASCADE;

/* Test automatic partition creation */
CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL);
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '10 days'::INTERVAL, 1);
INSERT INTO test.range_rel (dt)
SELECT generate_series('2015-01-01', '2015-04-30', '1 day'::interval);

INSERT INTO test.range_rel (dt)
SELECT generate_series('2014-12-31', '2014-12-01', '-1 day'::interval);

EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt = '2014-12-15';
SELECT * FROM test.range_rel WHERE dt = '2014-12-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt = '2015-03-15';
SELECT * FROM test.range_rel WHERE dt = '2015-03-15';

DROP TABLE test.range_rel CASCADE;
SELECT * FROM pathman.pathman_config;

/* Check overlaps */
CREATE TABLE test.num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);
SELECT pathman.create_range_partitions('test.num_range_rel', 'id', 1000, 1000, 4);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 4001, 5000);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 4000, 5000);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 3999, 5000);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 3000, 3500);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 0, 999);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 0, 1000);
SELECT pathman.check_overlap('test.num_range_rel'::regclass::oid, 0, 1001);

DROP EXTENSION pg_pathman;

/* Test that everithing works fine without schemas */
CREATE EXTENSION pg_pathman;

/* Hash */
CREATE TABLE hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER NOT NULL);
INSERT INTO hash_rel (value) SELECT g FROM generate_series(1, 10000) as g;
SELECT create_hash_partitions('hash_rel', 'value', 3);
EXPLAIN (COSTS OFF) SELECT * FROM hash_rel WHERE id = 1234;

/* Range */
CREATE TABLE range_rel (
	id		SERIAL PRIMARY KEY,
	dt		TIMESTAMP NOT NULL,
	value	INTEGER);
INSERT INTO range_rel (dt, value) SELECT g, extract(day from g) FROM generate_series('2010-01-01'::date, '2010-12-31'::date, '1 day') as g;
SELECT create_range_partitions('range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 12);
SELECT merge_range_partitions('range_rel_1', 'range_rel_2');
SELECT split_range_partition('range_rel_1', '2010-02-15'::date);
SELECT append_range_partition('range_rel');
SELECT prepend_range_partition('range_rel');
EXPLAIN (COSTS OFF) SELECT * FROM range_rel WHERE dt < '2010-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM range_rel WHERE dt > '2010-12-15';

/* Temporary table for JOINs */
CREATE TABLE tmp (id INTEGER NOT NULL, value INTEGER NOT NULL);
INSERT INTO tmp VALUES (1, 1), (2, 2);

/* Test UPDATE and DELETE */
EXPLAIN (COSTS OFF) UPDATE range_rel SET value = 111 WHERE dt = '2010-06-15';
UPDATE range_rel SET value = 111 WHERE dt = '2010-06-15';
SELECT * FROM range_rel WHERE dt = '2010-06-15';
EXPLAIN (COSTS OFF) DELETE FROM range_rel WHERE dt = '2010-06-15';
DELETE FROM range_rel WHERE dt = '2010-06-15';
SELECT * FROM range_rel WHERE dt = '2010-06-15';
EXPLAIN (COSTS OFF) UPDATE range_rel r SET value = t.value FROM tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
UPDATE range_rel r SET value = t.value FROM tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
EXPLAIN (COSTS OFF) DELETE FROM range_rel r USING tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;
DELETE FROM range_rel r USING tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;

/* Create range partitions from whole range */
SELECT drop_range_partitions('range_rel');
SELECT create_partitions_from_range('range_rel', 'id', 1, 1000, 100);
SELECT drop_range_partitions('range_rel', TRUE);
SELECT create_partitions_from_range('range_rel', 'dt', '2015-01-01'::date, '2015-12-01'::date, '1 month'::interval);
EXPLAIN (COSTS OFF) SELECT * FROM range_rel WHERE dt = '2015-12-15';

CREATE TABLE messages(id SERIAL PRIMARY KEY, msg TEXT);
CREATE TABLE replies(id SERIAL PRIMARY KEY, message_id INTEGER REFERENCES messages(id),  msg TEXT);
INSERT INTO messages SELECT g, md5(g::text) FROM generate_series(1, 10) as g;
INSERT INTO replies SELECT g, g, md5(g::text) FROM generate_series(1, 10) as g;
SELECT create_range_partitions('messages', 'id', 1, 100, 2);
ALTER TABLE replies DROP CONSTRAINT replies_message_id_fkey;
SELECT create_range_partitions('messages', 'id', 1, 100, 2);
EXPLAIN (COSTS OFF) SELECT * FROM messages;

DROP EXTENSION pg_pathman;
