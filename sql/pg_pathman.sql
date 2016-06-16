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

SELECT * FROM ONLY test.range_rel UNION SELECT * FROM test.range_rel;

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
-- Temporarily commented out
-- EXPLAIN (COSTS OFF) SELECT * FROM test.hash_rel WHERE value BETWEEN 1 AND 2;
--                    QUERY PLAN
-- -------------------------------------------------
--  Append
--    ->  Seq Scan on hash_rel_1
--          Filter: ((value >= 1) AND (value <= 2))
--    ->  Seq Scan on hash_rel_2
--          Filter: ((value >= 1) AND (value <= 2))
-- (5 rows)
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
 * Test CTE query
 */
EXPLAIN (COSTS OFF)
    WITH ttt AS (SELECT * FROM test.range_rel WHERE dt >= '2015-02-01' AND dt < '2015-03-15')
SELECT * FROM ttt;

EXPLAIN (COSTS OFF)
    WITH ttt AS (SELECT * FROM test.hash_rel WHERE value = 2)
SELECT * FROM ttt;


/*
 * Test RuntimeAppend
 */

create or replace function test.pathman_assert(smt bool, error_msg text) returns text as $$
begin
	if not smt then
		raise exception '%', error_msg;
	end if;

	return 'ok';
end;
$$ language plpgsql;

create or replace function test.pathman_equal(a text, b text, error_msg text) returns text as $$
begin
	if a != b then
		raise exception '''%'' is not equal to ''%'', %', a, b, error_msg;
	end if;

	return 'equal';
end;
$$ language plpgsql;

create or replace function test.pathman_test(query text) returns jsonb as $$
declare
	plan jsonb;
begin
	execute 'explain (analyze, format json)' || query into plan;

	return plan;
end;
$$ language plpgsql;

create or replace function test.pathman_test_1() returns text as $$
declare
	plan jsonb;
	num int;
begin
	plan = test.pathman_test('select * from test.runtime_test_1 where id = (select * from test.run_values limit 1)');

	perform test.pathman_equal((plan->0->'Plan'->'Node Type')::text,
							   '"Custom Scan"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Custom Plan Provider')::text,
							   '"RuntimeAppend"',
							   'wrong plan provider');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Relation Name')::text,
							   format('"runtime_test_1_%s"', pathman.get_hash(hashint4(1), 6)),
							   'wrong partition');

	select count(*) from jsonb_array_elements_text(plan->0->'Plan'->'Plans') into num;
	perform test.pathman_equal(num::text, '2', 'expected 2 child plans for custom scan');

	return 'ok';
end;
$$ language plpgsql;

create or replace function test.pathman_test_2() returns text as $$
declare
	plan jsonb;
	num int;
begin
	plan = test.pathman_test('select * from test.runtime_test_1 where id = any (select * from test.run_values limit 4)');

	perform test.pathman_equal((plan->0->'Plan'->'Node Type')::text,
							   '"Nested Loop"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Node Type')::text,
							   '"Custom Scan"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Custom Plan Provider')::text,
							   '"RuntimeAppend"',
							   'wrong plan provider');

	select count(*) from jsonb_array_elements_text(plan->0->'Plan'->'Plans'->1->'Plans') into num;
	perform test.pathman_equal(num::text, '4', 'expected 4 child plans for custom scan');

	for i in 0..3 loop
		perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Plans'->i->'Relation Name')::text,
								   format('"runtime_test_1_%s"', pathman.get_hash(hashint4(i + 1), 6)),
								   'wrong partition');

		num = plan->0->'Plan'->'Plans'->1->'Plans'->i->'Actual Loops';
		perform test.pathman_equal(num::text, '1', 'expected 1 loop');
	end loop;

	return 'ok';
end;
$$ language plpgsql;

create or replace function test.pathman_test_3() returns text as $$
declare
	plan jsonb;
	num int;
begin
	plan = test.pathman_test('select * from test.runtime_test_1 a join test.run_values b on a.id = b.val');

	perform test.pathman_equal((plan->0->'Plan'->'Node Type')::text,
							   '"Nested Loop"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Node Type')::text,
							   '"Custom Scan"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Custom Plan Provider')::text,
							   '"RuntimeAppend"',
							   'wrong plan provider');

	select count(*) from jsonb_array_elements_text(plan->0->'Plan'->'Plans'->1->'Plans') into num;
	perform test.pathman_equal(num::text, '6', 'expected 6 child plans for custom scan');

	for i in 0..5 loop
		num = plan->0->'Plan'->'Plans'->1->'Plans'->i->'Actual Loops';
		perform test.pathman_assert(num > 0 and num <= 1718, 'expected no more than 1718 loops');
	end loop;

	return 'ok';
end;
$$ language plpgsql;

create or replace function test.pathman_test_4() returns text as $$
declare
	plan jsonb;
	num int;
begin
	plan = test.pathman_test('select * from test.category c, lateral' ||
							 '(select * from test.runtime_test_2 g where g.category_id = c.id order by rating limit 4) as tg');

	perform test.pathman_equal((plan->0->'Plan'->'Node Type')::text,
							   '"Nested Loop"',
							   'wrong plan type');

														/* Limit -> Custom Scan */
	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->0->'Node Type')::text,
							   '"Custom Scan"',
							   'wrong plan type');

	perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->0->'Custom Plan Provider')::text,
							   '"RuntimeMergeAppend"',
							   'wrong plan provider');

	select count(*) from jsonb_array_elements_text(plan->0->'Plan'->'Plans'->1->'Plans'->0->'Plans') into num;
	perform test.pathman_equal(num::text, '4', 'expected 4 child plans for custom scan');

	for i in 0..3 loop
		perform test.pathman_equal((plan->0->'Plan'->'Plans'->1->'Plans'->0->'Plans'->i->'Relation Name')::text,
								   format('"runtime_test_2_%s"', pathman.get_hash(hashint4(i + 1), 6)),
								   'wrong partition');

		num = plan->0->'Plan'->'Plans'->1->'Plans'->0->'Plans'->i->'Actual Loops';
		perform test.pathman_assert(num = 1, 'expected no more than 1 loops');
	end loop;

	return 'ok';
end;
$$ language plpgsql;


create table test.run_values as select generate_series(1, 10000) val;
create table test.runtime_test_1(id serial primary key, val real);
insert into test.runtime_test_1 select generate_series(1, 10000), random();
select pathman.create_hash_partitions('test.runtime_test_1', 'id', 6);

create table test.category as (select id, 'cat' || id::text as name from generate_series(1, 4) id);
create table test.runtime_test_2 (id serial, category_id int not null, name text, rating real);
insert into test.runtime_test_2 (select id, (id % 6) + 1 as category_id, 'good' || id::text as name, random() as rating from generate_series(1, 100000) id);
create index on test.runtime_test_2 (category_id, rating);
select pathman.create_hash_partitions('test.runtime_test_2', 'category_id', 6);

analyze test.run_values;
analyze test.runtime_test_1;

set enable_mergejoin = off;
set enable_hashjoin = off;
set pg_pathman.enable_runtimeappend = on;
set pg_pathman.enable_runtimemergeappend = on;
select test.pathman_test_1(); /* RuntimeAppend (select ... where id = (subquery)) */
select test.pathman_test_2(); /* RuntimeAppend (select ... where id = any(subquery)) */
select test.pathman_test_3(); /* RuntimeAppend (a join b on a.id = b.val) */
select test.pathman_test_4(); /* RuntimeMergeAppend (lateral) */

set pg_pathman.enable_runtimeappend = off;
set pg_pathman.enable_runtimemergeappend = off;
set enable_mergejoin = on;
set enable_hashjoin = on;

drop table test.run_values, test.runtime_test_1, test.runtime_test_2 cascade;

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
 * Check that altering table columns doesn't break trigger
 */
ALTER TABLE test.hash_rel ADD COLUMN abc int;
INSERT INTO test.hash_rel (id, value, abc) VALUES (123, 456, 789);
SELECT * FROM test.hash_rel WHERE id = 123;

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
SELECT pathman.drop_hash_partitions('test."TeSt"');
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
SELECT pathman.drop_range_partitions('test."RangeRel"');
SELECT pathman.create_partitions_from_range('test."RangeRel"', 'dt', '2015-01-01'::DATE, '2015-01-05'::DATE, '1 day'::INTERVAL);
DROP TABLE test."RangeRel" CASCADE;
SELECT * FROM pathman.pathman_config;
CREATE TABLE test."RangeRel" (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
SELECT pathman.create_range_partitions('test."RangeRel"', 'id', 1, 100, 3);
SELECT pathman.drop_range_partitions('test."RangeRel"');
SELECT pathman.create_partitions_from_range('test."RangeRel"', 'id', 1, 300, 100);
DROP TABLE test."RangeRel" CASCADE;

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
