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
SELECT pathman.create_hash_partitions('test.hash_rel', 'value', 3);
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
SELECT pathman.create_range_partitions('test.range_rel', 'dt', '2015-01-01'::DATE, '1 month'::INTERVAL, 2);
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


/* Test INSERT hooking with DATE type */
CREATE TABLE test.insert_date_test(val DATE NOT NULL);
SELECT pathman.create_partitions_from_range('test.insert_date_test', 'val',
											date '20161001', date '20170101', interval '1 month');

INSERT INTO test.insert_date_test VALUES ('20161201'); /* just insert the date */
SELECT count(*) FROM pathman.pathman_partition_list WHERE parent = 'test.insert_date_test'::REGCLASS;

INSERT INTO test.insert_date_test VALUES ('20170311'); /* append new partitions */
SELECT count(*) FROM pathman.pathman_partition_list WHERE parent = 'test.insert_date_test'::REGCLASS;

INSERT INTO test.insert_date_test VALUES ('20160812'); /* prepend new partitions */
SELECT count(*) FROM pathman.pathman_partition_list WHERE parent = 'test.insert_date_test'::REGCLASS;

SELECT min(val) FROM test.insert_date_test; /* check first date */
SELECT max(val) FROM test.insert_date_test; /* check last date */

DROP TABLE test.insert_date_test CASCADE;


/* Test special case: ONLY statement with not-ONLY for partitioned table */
CREATE TABLE test.from_only_test(val INT NOT NULL);
INSERT INTO test.from_only_test SELECT generate_series(1, 20);
SELECT pathman.create_range_partitions('test.from_only_test', 'val', 1, 2);

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM ONLY test.from_only_test
UNION SELECT * FROM test.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM test.from_only_test
UNION SELECT * FROM ONLY test.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM test.from_only_test
UNION SELECT * FROM test.from_only_test
UNION SELECT * FROM ONLY test.from_only_test;

/* should be OK */
EXPLAIN (COSTS OFF)
SELECT * FROM ONLY test.from_only_test
UNION SELECT * FROM test.from_only_test
UNION SELECT * FROM test.from_only_test;

/* not ok, ONLY|non-ONLY in one query */
EXPLAIN (COSTS OFF)
SELECT * FROM test.from_only_test a JOIN ONLY test.from_only_test b USING(val);

EXPLAIN (COSTS OFF)
WITH q1 AS (SELECT * FROM test.from_only_test),
	 q2 AS (SELECT * FROM ONLY test.from_only_test)
SELECT * FROM q1 JOIN q2 USING(val);

EXPLAIN (COSTS OFF)
WITH q1 AS (SELECT * FROM ONLY test.from_only_test)
SELECT * FROM test.from_only_test JOIN q1 USING(val);

EXPLAIN (COSTS OFF)
SELECT * FROM test.range_rel WHERE id = (SELECT id FROM ONLY test.range_rel LIMIT 1);

DROP TABLE test.from_only_test CASCADE;


SET pg_pathman.enable_runtimeappend = OFF;
SET pg_pathman.enable_runtimemergeappend = OFF;

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
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id IN (2500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id IN (500, 1500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id IN (-500, 500, 1500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id IN (-1, -1, -1);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id IN (-1, -1, -1, NULL);
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
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (2);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (2, 1);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (1, 2);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (1, 2, -1);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (0, 0, 0);
EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value IN (NULL::int, NULL, NULL);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id > 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1000 AND id < 3000;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id >= 1500 AND id < 2500;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE (id >= 500 AND id < 1500) OR (id > 2500);
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel ORDER BY id;
EXPLAIN (COSTS OFF) SELECT * FROM test.num_range_rel WHERE id <= 2500 ORDER BY id;
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt > '2015-02-15';
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
SET enable_hashjoin = OFF;
set enable_nestloop = OFF;
SET enable_mergejoin = ON;

EXPLAIN (COSTS OFF)
SELECT * FROM test.range_rel j1
JOIN test.range_rel j2 on j2.id = j1.id
JOIN test.num_range_rel j3 on j3.id = j1.id
WHERE j1.dt < '2015-03-01' AND j2.dt >= '2015-02-01' ORDER BY j2.dt;
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
 * Test CTE query
 */
EXPLAIN (COSTS OFF)
	WITH ttt AS (SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-15')
SELECT * FROM ttt;

EXPLAIN (COSTS OFF)
	WITH ttt AS (SELECT * FROM test.hash_rel WHERE value = 2)
SELECT * FROM ttt;

/*
 * Test CTE query - by @parihaaraka (add varno to WalkerContext)
 */
CREATE TABLE test.cte_del_xacts (id BIGSERIAL PRIMARY KEY, pdate DATE NOT NULL);
INSERT INTO test.cte_del_xacts (pdate) SELECT gen_date FROM generate_series('2016-01-01'::date, '2016-04-9'::date, '1 day') AS gen_date;

create table test.cte_del_xacts_specdata
(
	tid BIGINT PRIMARY KEY,
	test_mode SMALLINT,
	state_code SMALLINT NOT NULL DEFAULT 8,
	regtime TIMESTAMP WITHOUT TIME ZONE NOT NULL
);
INSERT INTO test.cte_del_xacts_specdata VALUES(1, 1, 1, current_timestamp); /* for subquery test */

/* create 2 partitions */
SELECT pathman.create_range_partitions('test.cte_del_xacts'::regclass, 'pdate', '2016-01-01'::date, '50 days'::interval);

EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test.cte_del_xacts_specdata)
DELETE FROM test.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

SELECT pathman.drop_partitions('test.cte_del_xacts'); /* now drop partitions */

/* create 1 partition */
SELECT pathman.create_range_partitions('test.cte_del_xacts'::regclass, 'pdate', '2016-01-01'::date, '1 year'::interval);

/* parent enabled! */
SELECT pathman.set_enable_parent('test.cte_del_xacts', true);
EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test.cte_del_xacts_specdata)
DELETE FROM test.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

/* parent disabled! */
SELECT pathman.set_enable_parent('test.cte_del_xacts', false);
EXPLAIN (COSTS OFF)
WITH tmp AS (
	SELECT tid, test_mode, regtime::DATE AS pdate, state_code
	FROM test.cte_del_xacts_specdata)
DELETE FROM test.cte_del_xacts t USING tmp
WHERE t.id = tmp.tid AND t.pdate = tmp.pdate AND tmp.test_mode > 0;

/* create stub pl/PgSQL function */
CREATE OR REPLACE FUNCTION test.cte_del_xacts_stab(name TEXT)
RETURNS smallint AS
$$
begin
	return 2::smallint;
end
$$
LANGUAGE plpgsql STABLE;

/* test subquery planning */
WITH tmp AS (
	SELECT tid FROM test.cte_del_xacts_specdata
	WHERE state_code != test.cte_del_xacts_stab('test'))
SELECT * FROM test.cte_del_xacts t JOIN tmp ON t.id = tmp.tid;

/* test subquery planning (one more time) */
WITH tmp AS (
	SELECT tid FROM test.cte_del_xacts_specdata
	WHERE state_code != test.cte_del_xacts_stab('test'))
SELECT * FROM test.cte_del_xacts t JOIN tmp ON t.id = tmp.tid;

DROP FUNCTION test.cte_del_xacts_stab(TEXT);
DROP TABLE test.cte_del_xacts, test.cte_del_xacts_specdata CASCADE;


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
\d+ test.hash_rel_0
\d+ test.hash_rel_extern
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
SELECT pathman.create_hash_update_trigger('test."TeSt"');
UPDATE test."TeSt" SET a = 1;
SELECT * FROM test."TeSt";
SELECT * FROM test."TeSt" WHERE a = 1;
EXPLAIN (COSTS OFF) SELECT * FROM test."TeSt" WHERE a = 1;
SELECT pathman.drop_partitions('test."TeSt"');
SELECT * FROM test."TeSt";

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
SELECT pathman.drop_partitions('test."RangeRel"');
SELECT pathman.create_partitions_from_range('test."RangeRel"', 'dt', '2015-01-01'::DATE, '2015-01-05'::DATE, '1 day'::INTERVAL);
DROP TABLE test."RangeRel" CASCADE;
SELECT * FROM pathman.pathman_config;
CREATE TABLE test."RangeRel" (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
SELECT pathman.create_range_partitions('test."RangeRel"', 'id', 1, 100, 3);
SELECT pathman.drop_partitions('test."RangeRel"');
SELECT pathman.create_partitions_from_range('test."RangeRel"', 'id', 1, 300, 100);
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
EXPLAIN (COSTS OFF) UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';
UPDATE test.range_rel SET value = 111 WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';
EXPLAIN (COSTS OFF) DELETE FROM test.range_rel WHERE dt = '2010-06-15';
DELETE FROM test.range_rel WHERE dt = '2010-06-15';
SELECT * FROM test.range_rel WHERE dt = '2010-06-15';
EXPLAIN (COSTS OFF) UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
UPDATE test.range_rel r SET value = t.value FROM test.tmp t WHERE r.dt = '2010-01-01' AND r.id = t.id;
EXPLAIN (COSTS OFF) DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;
DELETE FROM test.range_rel r USING test.tmp t WHERE r.dt = '2010-01-02' AND r.id = t.id;

/* Create range partitions from whole range */
SELECT drop_partitions('test.range_rel');
SELECT create_partitions_from_range('test.range_rel', 'id', 1, 1000, 100);
SELECT drop_partitions('test.range_rel', TRUE);
SELECT create_partitions_from_range('test.range_rel', 'dt', '2015-01-01'::date, '2015-12-01'::date, '1 month'::interval);
EXPLAIN (COSTS OFF) SELECT * FROM test.range_rel WHERE dt = '2015-12-15';

/* Test NOT operator */
CREATE TABLE bool_test(a INT NOT NULL, b BOOLEAN);
SELECT create_hash_partitions('bool_test', 'a', 3);
INSERT INTO bool_test SELECT g, (g % 4) = 0 FROM generate_series(1, 100) AS g;
SELECT count(*) FROM bool_test;
SELECT count(*) FROM bool_test WHERE (b = true AND b = false);
SELECT count(*) FROM bool_test WHERE b = false;	/* 75 values */
SELECT count(*) FROM bool_test WHERE b = true;	/* 25 values */
DROP TABLE bool_test CASCADE;

/* Test foreign keys */
CREATE TABLE test.messages(id SERIAL PRIMARY KEY, msg TEXT);
CREATE TABLE test.replies(id SERIAL PRIMARY KEY, message_id INTEGER REFERENCES test.messages(id),  msg TEXT);
INSERT INTO test.messages SELECT g, md5(g::text) FROM generate_series(1, 10) as g;
INSERT INTO test.replies SELECT g, g, md5(g::text) FROM generate_series(1, 10) as g;
SELECT create_range_partitions('test.messages', 'id', 1, 100, 2);
ALTER TABLE test.replies DROP CONSTRAINT replies_message_id_fkey;
SELECT create_range_partitions('test.messages', 'id', 1, 100, 2);
EXPLAIN (COSTS OFF) SELECT * FROM test.messages;
DROP TABLE test.messages, test.replies CASCADE;

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

/* Test recursive CTE */
CREATE TABLE test.recursive_cte_test_tbl(id INT NOT NULL, name TEXT NOT NULL);
SELECT * FROM create_hash_partitions('test.recursive_cte_test_tbl', 'id', 2);
INSERT INTO test.recursive_cte_test_tbl (id, name) SELECT id, 'name'||id FROM generate_series(1,100) f(id);
INSERT INTO test.recursive_cte_test_tbl (id, name) SELECT id, 'name'||(id + 1) FROM generate_series(1,100) f(id);
INSERT INTO test.recursive_cte_test_tbl (id, name) SELECT id, 'name'||(id + 2) FROM generate_series(1,100) f(id);
SELECT * FROM test.recursive_cte_test_tbl WHERE id = 5;

WITH RECURSIVE test AS (
	SELECT min(name) AS name
	FROM test.recursive_cte_test_tbl
	WHERE id = 5
	UNION ALL
	SELECT (SELECT min(name)
			FROM test.recursive_cte_test_tbl
			WHERE id = 5 AND name > test.name)
	FROM test
	WHERE name IS NOT NULL)
SELECT * FROM test;


/* Test create_range_partitions() + relnames */
CREATE TABLE test.provided_part_names(id INT NOT NULL);
INSERT INTO test.provided_part_names SELECT generate_series(1, 10);
SELECT create_hash_partitions('test.provided_part_names', 'id', 2,
							  relnames := ARRAY[]::TEXT[]);				/* not ok */
SELECT create_hash_partitions('test.provided_part_names', 'id', 2,
							  relnames := ARRAY['p1', 'p2']::TEXT[]);	/* ok */
/* list partitions */
SELECT partition FROM pathman_partition_list
WHERE parent = 'test.provided_part_names'::REGCLASS
ORDER BY partition;

DROP TABLE test.provided_part_names CASCADE;


DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;
