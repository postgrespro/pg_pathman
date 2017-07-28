\set VERBOSITY terse

SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

CREATE TABLE test.hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER);
INSERT INTO test.hash_rel VALUES (1, 1);
INSERT INTO test.hash_rel VALUES (2, 2);
INSERT INTO test.hash_rel VALUES (3, 3);

\set VERBOSITY default
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
\set VERBOSITY terse

ALTER TABLE test.hash_rel ALTER COLUMN value SET NOT NULL;

SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3, partition_data:=false);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
SELECT * FROM test.hash_rel;
SELECT pathman.set_enable_parent('test.hash_rel', false);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
SELECT * FROM test.hash_rel;
SELECT pathman.set_enable_parent('test.hash_rel', true);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
SELECT * FROM test.hash_rel;
SELECT pathman.drop_partitions('test.hash_rel');
SELECT pathman.create_hash_partitions('test.hash_rel', 'Value', 3);
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

\set VERBOSITY default
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL);
\set VERBOSITY terse

ALTER TABLE test.range_rel ALTER COLUMN dt SET NOT NULL;

SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL, 2);
SELECT pathman.create_range_partitions('test.range_rel', 'DT', '2015-01-01'::DATE, '1 month'::INTERVAL);
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


/* since rel_1_2_beta: check append_child_relation(), make_ands_explicit(), dummy path */
CREATE TABLE test.improved_dummy (id BIGSERIAL, name TEXT NOT NULL);
INSERT INTO test.improved_dummy (name) SELECT md5(g::TEXT) FROM generate_series(1, 100) as g;
SELECT pathman.create_range_partitions('test.improved_dummy', 'id', 1, 10);
INSERT INTO test.improved_dummy (name) VALUES ('test'); /* spawns new partition */

EXPLAIN (COSTS OFF) SELECT * FROM test.improved_dummy WHERE id = 101 OR id = 5 AND name = 'ib';
SELECT pathman.set_enable_parent('test.improved_dummy', true); /* enable parent */
EXPLAIN (COSTS OFF) SELECT * FROM test.improved_dummy WHERE id = 101 OR id = 5 AND name = 'ib';
SELECT pathman.set_enable_parent('test.improved_dummy', false); /* disable parent */

ALTER TABLE test.improved_dummy_1 ADD CHECK (name != 'ib'); /* make test.improved_dummy_1 disappear */

EXPLAIN (COSTS OFF) SELECT * FROM test.improved_dummy WHERE id = 101 OR id = 5 AND name = 'ib';
SELECT pathman.set_enable_parent('test.improved_dummy', true); /* enable parent */
EXPLAIN (COSTS OFF) SELECT * FROM test.improved_dummy WHERE id = 101 OR id = 5 AND name = 'ib';

DROP TABLE test.improved_dummy CASCADE;


/* since rel_1_4_beta: check create_range_partitions(bounds array) */
CREATE TABLE test.improved_dummy (val INT NOT NULL);

SELECT pathman.create_range_partitions('test.improved_dummy', 'val',
									   pathman.generate_range_bounds(1, 1, 2));

SELECT * FROM pathman.pathman_partition_list
WHERE parent = 'test.improved_dummy'::REGCLASS
ORDER BY partition;

SELECT pathman.drop_partitions('test.improved_dummy');

SELECT pathman.create_range_partitions('test.improved_dummy', 'val',
									   pathman.generate_range_bounds(1, 1, 2),
									   partition_names := '{p1, p2}');

SELECT * FROM pathman.pathman_partition_list
WHERE parent = 'test.improved_dummy'::REGCLASS
ORDER BY partition;

SELECT pathman.drop_partitions('test.improved_dummy');

SELECT pathman.create_range_partitions('test.improved_dummy', 'val',
									   pathman.generate_range_bounds(1, 1, 2),
									   partition_names := '{p1, p2}',
									   tablespaces := '{pg_default, pg_default}');

SELECT * FROM pathman.pathman_partition_list
WHERE parent = 'test.improved_dummy'::REGCLASS
ORDER BY partition;

DROP TABLE test.improved_dummy CASCADE;


/* Test pathman_rel_pathlist_hook() with INSERT query */
CREATE TABLE test.insert_into_select(val int NOT NULL);
INSERT INTO test.insert_into_select SELECT generate_series(1, 100);
SELECT pathman.create_range_partitions('test.insert_into_select', 'val', 1, 20);
CREATE TABLE test.insert_into_select_copy (LIKE test.insert_into_select); /* INSERT INTO ... SELECT ... */

EXPLAIN (COSTS OFF)
INSERT INTO test.insert_into_select_copy
SELECT * FROM test.insert_into_select
WHERE val <= 80;

SELECT pathman.set_enable_parent('test.insert_into_select', true);

EXPLAIN (COSTS OFF)
INSERT INTO test.insert_into_select_copy
SELECT * FROM test.insert_into_select
WHERE val <= 80;

INSERT INTO test.insert_into_select_copy SELECT * FROM test.insert_into_select;
SELECT count(*) FROM test.insert_into_select_copy;
DROP TABLE test.insert_into_select_copy, test.insert_into_select CASCADE;


SET pg_pathman.enable_runtimeappend = OFF;
SET pg_pathman.enable_runtimemergeappend = OFF;

VACUUM;

SET enable_indexscan = OFF;
SET enable_bitmapscan = OFF;
SET enable_seqscan = ON;

EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE false;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = NULL;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE 2 = value; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2 OR value = 1;

EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE 2500 = id; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE 2500 < id; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);

EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2015-02-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE '2015-02-15' < dt; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-15' AND dt < '2015-03-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE (dt >= '2015-01-15' AND dt < '2015-02-15') OR (dt > '2015-03-15');


SET enable_indexscan = ON;
SET enable_bitmapscan = OFF;
SET enable_seqscan = OFF;

EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE false;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = NULL;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2;
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE 2 = value; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value = 2 OR value = 1;

EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE 2500 = id; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE 2500 < id; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel ORDER BY id;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id <= 2500 ORDER BY id;

EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2015-02-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE '2015-02-15' < dt; /* test commutator */
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-02-15' AND dt < '2015-03-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE (dt >= '2015-01-15' AND dt < '2015-02-15') OR (dt > '2015-03-15');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel ORDER BY dt;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-01-15' ORDER BY dt DESC;

/*
 * Sorting
 */
SET enable_indexscan = OFF;
SET enable_seqscan = ON;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt < '2015-03-01' ORDER BY dt;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel_1 UNION ALL SELECT * FROM test.range_rel_2 ORDER BY dt;
SET enable_indexscan = ON;
SET enable_seqscan = OFF;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt < '2015-03-01' ORDER BY dt;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel_1 UNION ALL SELECT * FROM test.range_rel_2 ORDER BY dt;

/*
 * Join
 */
set enable_nestloop = OFF;
SET enable_hashjoin = ON;
SET enable_mergejoin = OFF;
EXPLAIN (COSTS OFF)
SELECT * FROM test.range_rel j1
JOIN test.range_rel j2 on j2.id = j1.id
JOIN test.num_range_rel j3 on j3.id = j1.id
WHERE j1.dt < '2015-03-01' AND j2.dt >= '2015-02-01' ORDER BY j2.dt;

/*
 * Test inlined SQL functions
 */
CREATE TABLE test.sql_inline (id INT NOT NULL);
SELECT pathman.create_hash_partitions('test.sql_inline', 'id', 3);

CREATE OR REPLACE FUNCTION test.sql_inline_func(i_id int) RETURNS SETOF INT AS $$
	select * from test.sql_inline where id = i_id limit 1;
$$ LANGUAGE sql STABLE;

EXPLAIN (COSTS OFF) SELECT * FROM test.sql_inline_func(5);
EXPLAIN (COSTS OFF) SELECT * FROM test.sql_inline_func(1);

DROP FUNCTION test.sql_inline_func(int);
DROP TABLE test.sql_inline CASCADE;

/*
 * Test by @baiyinqiqi (issue #60)
 */
CREATE TABLE test.hash_varchar(val VARCHAR(40) NOT NULL);
INSERT INTO test.hash_varchar SELECT generate_series(1, 20);

SELECT pathman.create_hash_partitions('test.hash_varchar', 'val', 4);
SELECT * FROM test.hash_varchar WHERE val = 'a';
SELECT * FROM test.hash_varchar WHERE val = '12'::TEXT;

DROP TABLE test.hash_varchar CASCADE;


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
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 4000;
SELECT pathman.prepend_range_partition('test.num_range_rel');
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id < 0;
SELECT pathman.drop_range_partition('test.num_range_rel_7');

SELECT pathman.drop_range_partition_expand_next('test.num_range_rel_4');
SELECT * FROM pathman.pathman_partition_list WHERE parent = 'test.num_range_rel'::regclass;
SELECT pathman.drop_range_partition_expand_next('test.num_range_rel_6');
SELECT * FROM pathman.pathman_partition_list WHERE parent = 'test.num_range_rel'::regclass;

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

/* Half open ranges */
SELECT pathman.add_range_partition('test.range_rel', NULL, '2014-12-01'::DATE, 'test.range_rel_minus_infinity');
SELECT pathman.add_range_partition('test.range_rel', '2015-06-01'::DATE, NULL, 'test.range_rel_plus_infinity');
SELECT pathman.append_range_partition('test.range_rel');
SELECT pathman.prepend_range_partition('test.range_rel');
DROP TABLE test.range_rel_minus_infinity;
CREATE TABLE test.range_rel_minus_infinity (LIKE test.range_rel INCLUDING ALL);
SELECT pathman.attach_range_partition('test.range_rel', 'test.range_rel_minus_infinity', NULL, '2014-12-01'::DATE);
SELECT * FROM pathman.pathman_partition_list WHERE parent = 'test.range_rel'::REGCLASS;
INSERT INTO test.range_rel (dt) VALUES ('2012-06-15');
INSERT INTO test.range_rel (dt) VALUES ('2015-12-15');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt < '2015-01-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt >= '2015-05-01';

/*
 * Zero partitions count and adding partitions with specified name
 */
CREATE TABLE test.zero(
	id		SERIAL PRIMARY KEY,
	value	INT NOT NULL);
INSERT INTO test.zero SELECT g, g FROM generate_series(1, 100) as g;
SELECT pathman.create_range_partitions('test.zero', 'value', 50, 10, 0);
SELECT pathman.append_range_partition('test.zero', 'test.zero_0');
SELECT pathman.prepend_range_partition('test.zero', 'test.zero_1');
SELECT pathman.add_range_partition('test.zero', 50, 70, 'test.zero_50');
SELECT pathman.append_range_partition('test.zero', 'test.zero_appended');
SELECT pathman.prepend_range_partition('test.zero', 'test.zero_prepended');
SELECT pathman.split_range_partition('test.zero_50', 60, 'test.zero_60');
DROP TABLE test.zero CASCADE;

/*
 * Check that altering table columns doesn't break trigger
 */
ALTER TABLE test.hash_rel ADD COLUMN abc int;
INSERT INTO test.hash_rel (id, value, abc) VALUES (123, 456, 789);
SELECT * FROM test.hash_rel WHERE id = 123;

/* Test replacing hash partition */
CREATE TABLE test.hash_rel_extern (LIKE test.hash_rel INCLUDING ALL);
SELECT pathman.replace_hash_partition('test.hash_rel_0', 'test.hash_rel_extern');

/* Check the consistency of test.hash_rel_0 and test.hash_rel_extern relations */
EXPLAIN(COSTS OFF) SELECT * FROM test.hash_rel;
SELECT parent, partition, parttype
FROM pathman.pathman_partition_list
WHERE parent='test.hash_rel'::regclass
ORDER BY 2;
SELECT c.oid::regclass::text,
    array_agg(pg_get_indexdef(i.indexrelid)) AS indexes,
    array_agg(pg_get_triggerdef(t.oid)) AS triggers
FROM pg_class c
    LEFT JOIN pg_index i ON c.oid=i.indrelid
    LEFT JOIN pg_trigger t ON c.oid=t.tgrelid
WHERE c.oid IN ('test.hash_rel_0'::regclass, 'test.hash_rel_extern'::regclass)
GROUP BY 1 ORDER BY 1;
SELECT pathman.is_tuple_convertible('test.hash_rel_0', 'test.hash_rel_extern');

INSERT INTO test.hash_rel SELECT * FROM test.hash_rel_0;
DROP TABLE test.hash_rel_0;
/* Table with which we are replacing partition must have exact same structure */
CREATE TABLE test.hash_rel_wrong(
	id		INTEGER NOT NULL,
	value	INTEGER);
SELECT pathman.replace_hash_partition('test.hash_rel_1', 'test.hash_rel_wrong');
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel;

/*
 * Clean up
 */
SELECT pathman.drop_partitions('test.hash_rel');
SELECT COUNT(*) FROM ONLY test.hash_rel;
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
SELECT pathman.drop_partitions('test.hash_rel', TRUE);
SELECT COUNT(*) FROM ONLY test.hash_rel;
DROP TABLE test.hash_rel CASCADE;

SELECT pathman.drop_partitions('test.num_range_rel');
DROP TABLE test.num_range_rel CASCADE;

DROP TABLE test.range_rel CASCADE;

/* Test attributes copying */
CREATE UNLOGGED TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	DATE NOT NULL)
WITH (fillfactor = 70);
INSERT INTO test.range_rel (dt)
	SELECT g FROM generate_series('2015-01-01', '2015-02-15', '1 month'::interval) AS g;
SELECT pathman.create_range_partitions('test.range_rel', 'dt',
	'2015-01-01'::date, '1 month'::interval);
SELECT reloptions, relpersistence FROM pg_class WHERE oid='test.range_rel'::REGCLASS;
SELECT reloptions, relpersistence FROM pg_class WHERE oid='test.range_rel_1'::REGCLASS;
DROP TABLE test.range_rel CASCADE;

/* Test automatic partition creation */
CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	data TEXT);
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '10 days'::INTERVAL, 1);
INSERT INTO test.range_rel (dt)
SELECT generate_series('2015-01-01', '2015-04-30', '1 day'::interval);

INSERT INTO test.range_rel (dt)
SELECT generate_series('2014-12-31', '2014-12-01', '-1 day'::interval);

EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt = '2014-12-15';
SELECT * FROM test.range_rel WHERE dt = '2014-12-15';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt = '2015-03-15';
SELECT * FROM test.range_rel WHERE dt = '2015-03-15';

SELECT pathman.set_auto('test.range_rel', false);
INSERT INTO test.range_rel (dt) VALUES ('2015-06-01');
SELECT pathman.set_auto('test.range_rel', true);
INSERT INTO test.range_rel (dt) VALUES ('2015-06-01');

/*
 * Test auto removing record from config on table DROP (but not on column drop
 * as it used to be before version 1.2)
 */
ALTER TABLE test.range_rel DROP COLUMN data;
SELECT * FROM pathman.pathman_config;
DROP TABLE test.range_rel CASCADE;
SELECT * FROM pathman.pathman_config;

/* Check overlaps */
CREATE TABLE test.num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);
SELECT pathman.create_range_partitions('test.num_range_rel', 'id', 1000, 1000, 4);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 4001, 5000);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 4000, 5000);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 3999, 5000);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 3000, 3500);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 0, 999);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 0, 1000);
SELECT pathman.check_range_available('test.num_range_rel'::regclass, 0, 1001);

/* CaMeL cAsE table names and attributes */
CREATE TABLE test."TeSt" (a INT NOT NULL, b INT);
SELECT pathman.create_hash_partitions('test.TeSt', 'a', 3);
SELECT pathman.create_hash_partitions('test."TeSt"', 'a', 3);
INSERT INTO test."TeSt" VALUES (1, 1);
INSERT INTO test."TeSt" VALUES (2, 2);
INSERT INTO test."TeSt" VALUES (3, 3);
SELECT * FROM test."TeSt";
SELECT pathman.create_update_triggers('test."TeSt"');
UPDATE test."TeSt" SET a = 1;
SELECT * FROM test."TeSt";
SELECT * FROM test."TeSt" WHERE a = 1;
EXPLAIN (COSTS OFF) SELECT * FROM test."TeSt" WHERE a = 1;
SELECT pathman.drop_partitions('test."TeSt"');
SELECT * FROM test."TeSt";
DROP TABLE test."TeSt" CASCADE;

CREATE TABLE test."RangeRel" (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
INSERT INTO test."RangeRel" (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-01-03', '1 day'::interval) as g;
SELECT pathman.create_range_partitions('test."RangeRel"', 'dt', '2015-01-01'::DATE, '1 day'::INTERVAL);
SELECT pathman.append_range_partition('test."RangeRel"');
SELECT pathman.prepend_range_partition('test."RangeRel"');
SELECT pathman.merge_range_partitions('test."RangeRel_1"', 'test."RangeRel_' || currval('test."RangeRel_seq"') || '"');
SELECT pathman.split_range_partition('test."RangeRel_1"', '2015-01-01'::DATE);
DROP TABLE test."RangeRel" CASCADE;
SELECT * FROM pathman.pathman_config;
CREATE TABLE test."RangeRel" (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
SELECT pathman.create_range_partitions('test."RangeRel"', 'id', 1, 100, 3);
DROP TABLE test."RangeRel" CASCADE;

DROP EXTENSION pg_pathman;


/* Test that everything works fine without schemas */
CREATE EXTENSION pg_pathman;

/* Hash */
CREATE TABLE test.hash_rel (
	id		SERIAL PRIMARY KEY,
	value	INTEGER NOT NULL);
INSERT INTO test.hash_rel (value) SELECT g FROM generate_series(1, 10000) as g;
SELECT create_hash_partitions('test.hash_rel', 'value', 3);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE id = 1234;

/* Range */
CREATE TABLE test.range_rel (
	id		SERIAL PRIMARY KEY,
	dt		TIMESTAMP NOT NULL,
	value	INTEGER);
INSERT INTO test.range_rel (dt, value) SELECT g, extract(day from g) FROM generate_series('2010-01-01'::date, '2010-12-31'::date, '1 day') as g;
SELECT create_range_partitions('test.range_rel', 'dt', '2010-01-01'::date, '1 month'::interval, 12);
SELECT merge_range_partitions('test.range_rel_1', 'test.range_rel_2');
SELECT split_range_partition('test.range_rel_1', '2010-02-15'::date);
SELECT append_range_partition('test.range_rel');
SELECT prepend_range_partition('test.range_rel');
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt < '2010-03-01';
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2010-12-15';

/* Temporary table for JOINs */
CREATE TABLE test.tmp (id INTEGER NOT NULL, value INTEGER NOT NULL);
INSERT INTO test.tmp VALUES (1, 1), (2, 2);


/* Test UPDATE and DELETE */
EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';	/* have partitions for this 'dt' */
UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt = '2010-06-15';	/* have partitions for this 'dt' */
DELETE FROM test.range_rel WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';

EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 222 WHERE dt = '1990-01-01';	/* no partitions for this 'dt' */
UPDATE test.range_rel SET value = 111 WHERE dt = '1990-01-01';
SELECT * FROM test.range_rel WHERE dt = '1990-01-01';

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt < '1990-01-01';	/* no partitions for this 'dt' */
DELETE FROM test.range_rel WHERE dt < '1990-01-01';
SELECT * FROM test.range_rel WHERE dt < '1990-01-01';

EXPLAIN (COSTS OFF) UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;

EXPLAIN (COSTS OFF) DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;
DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;


/* Create range partitions from whole range */
SELECT drop_partitions('test.range_rel');

/* Test NOT operator */
CREATE TABLE bool_test(a INT NOT NULL, b BOOLEAN);
SELECT create_hash_partitions('bool_test', 'a', 3);
INSERT INTO bool_test SELECT g, (g % 4) = 0 FROM generate_series(1, 100) AS g;
SELECT count(*) FROM bool_test;
SELECT count(*) FROM bool_test WHERE (b = true AND b = false);
SELECT count(*) FROM bool_test WHERE b = false;	/* 75 values */
SELECT count(*) FROM bool_test WHERE b = true;	/* 25 values */
DROP TABLE bool_test CASCADE;

/* Special test case (quals generation) -- fixing commit f603e6c5 */
CREATE TABLE test.special_case_1_ind_o_s(val serial, comment text);
INSERT INTO test.special_case_1_ind_o_s SELECT generate_series(1, 200), NULL;
SELECT create_range_partitions('test.special_case_1_ind_o_s', 'val', 1, 50);
INSERT INTO test.special_case_1_ind_o_s_2 SELECT 75 FROM generate_series(1, 6000);
CREATE INDEX ON test.special_case_1_ind_o_s_2 (val, comment);
VACUUM ANALYZE test.special_case_1_ind_o_s_2;
EXPLAIN (COSTS OFF) SELECT * FROM test.special_case_1_ind_o_s WHERE val < 75 AND comment = 'a';
SELECT set_enable_parent('test.special_case_1_ind_o_s', true);
EXPLAIN (COSTS OFF) SELECT * FROM test.special_case_1_ind_o_s WHERE val < 75 AND comment = 'a';
SELECT set_enable_parent('test.special_case_1_ind_o_s', false);
EXPLAIN (COSTS OFF) SELECT * FROM test.special_case_1_ind_o_s WHERE val < 75 AND comment = 'a';

/* Test index scans on child relation under enable_parent is set */
CREATE TABLE test.index_on_childs(c1 integer not null, c2 integer);
CREATE INDEX ON test.index_on_childs(c2);
INSERT INTO test.index_on_childs SELECT i, (random()*10000)::integer FROM generate_series(1, 10000) i;
SELECT create_range_partitions('test.index_on_childs', 'c1', 1, 1000, 0, false);
SELECT add_range_partition('test.index_on_childs', 1, 1000, 'test.index_on_childs_1_1k');
SELECT append_range_partition('test.index_on_childs', 'test.index_on_childs_1k_2k');
SELECT append_range_partition('test.index_on_childs', 'test.index_on_childs_2k_3k');
SELECT append_range_partition('test.index_on_childs', 'test.index_on_childs_3k_4k');
SELECT append_range_partition('test.index_on_childs', 'test.index_on_childs_4k_5k');
SELECT set_enable_parent('test.index_on_childs', true);
VACUUM ANALYZE test.index_on_childs;
EXPLAIN (COSTS OFF) SELECT * FROM test.index_on_childs WHERE c1 > 100 AND c1 < 2500 AND c2 = 500;

/* Test create_range_partitions() + partition_names */
CREATE TABLE test.provided_part_names(id INT NOT NULL);
INSERT INTO test.provided_part_names SELECT generate_series(1, 10);
SELECT create_hash_partitions('test.provided_part_names', 'id', 2,
							  partition_names := ARRAY['p1', 'p2']::TEXT[]); /* ok */
/* list partitions */
SELECT partition FROM pathman_partition_list
WHERE parent = 'test.provided_part_names'::REGCLASS
ORDER BY partition;

DROP TABLE test.provided_part_names CASCADE;


DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
