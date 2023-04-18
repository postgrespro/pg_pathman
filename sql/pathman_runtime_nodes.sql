\set VERBOSITY terse
SET search_path = 'public';
CREATE SCHEMA pathman;
CREATE EXTENSION pg_pathman SCHEMA pathman;
CREATE SCHEMA test;

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
							   format('"runtime_test_1_%s"', pathman.get_hash_part_idx(hashint4(1), 6)),
							   'wrong partition');

	select count(*) from jsonb_array_elements_text(plan->0->'Plan'->'Plans') into num;
	perform test.pathman_equal(num::text, '2', 'expected 2 child plans for custom scan');

	return 'ok';
end;
$$ language plpgsql
set enable_mergejoin = off
set enable_hashjoin = off;

create or replace function test.pathman_test_2() returns text as $$
declare
	plan jsonb;
	num int;
	c text;
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

	execute 'select string_agg(y.z, '','') from
				(select (x->''Relation Name'')::text as z from
					jsonb_array_elements($1->0->''Plan''->''Plans''->1->''Plans'') x
				 order by x->''Relation Name'') y'
		into c using plan;
	perform test.pathman_equal(c, '"runtime_test_1_2","runtime_test_1_3","runtime_test_1_4","runtime_test_1_5"',
								'wrong partitions');

	for i in 0..3 loop
		num = plan->0->'Plan'->'Plans'->1->'Plans'->i->'Actual Loops';
		perform test.pathman_equal(num::text, '1', 'expected 1 loop');
	end loop;

	return 'ok';
end;
$$ language plpgsql
set enable_mergejoin = off
set enable_hashjoin = off;

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
$$ language plpgsql
set enable_mergejoin = off
set enable_hashjoin = off;

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
								   format('"runtime_test_2_%s"', pathman.get_hash_part_idx(hashint4(i + 1), 6)),
								   'wrong partition');

		num = plan->0->'Plan'->'Plans'->1->'Plans'->0->'Plans'->i->'Actual Loops';
		perform test.pathman_assert(num = 1, 'expected no more than 1 loops');
	end loop;

	return 'ok';
end;
$$ language plpgsql
set enable_mergejoin = off
set enable_hashjoin = off;

create or replace function test.pathman_test_5() returns text as $$
declare
	res record;
begin
	select
	from test.runtime_test_3
	where id = (select * from test.vals order by val limit 1)
	limit 1
	into res; /* test empty tlist */


	select id * 2, id, 17
	from test.runtime_test_3
	where id = (select * from test.vals order by val limit 1)
	limit 1
	into res; /* test computations */


	select test.vals.* from test.vals, lateral (select from test.runtime_test_3
												where id = test.vals.val) as q
	into res; /* test lateral */


	select id, generate_series(1, 2) gen, val
	from test.runtime_test_3
	where id = (select * from test.vals order by val limit 1)
	order by id, gen, val
	offset 1 limit 1
	into res; /* without IndexOnlyScan */

	perform test.pathman_equal(res.id::text, '1', 'id is incorrect (t2)');
	perform test.pathman_equal(res.gen::text, '2', 'gen is incorrect (t2)');
	perform test.pathman_equal(res.val::text, 'k = 1', 'val is incorrect (t2)');


	select id
	from test.runtime_test_3
	where id = any (select * from test.vals order by val limit 5)
	order by id
	offset 3 limit 1
	into res; /* with IndexOnlyScan */

	perform test.pathman_equal(res.id::text, '4', 'id is incorrect (t3)');


	select v.val v1, generate_series(2, 2) gen, t.val v2
	from test.runtime_test_3 t join test.vals v on id = v.val
	order by v1, gen, v2
	limit 1
	into res;

	perform test.pathman_equal(res.v1::text, '1', 'v1 is incorrect (t4)');
	perform test.pathman_equal(res.gen::text, '2', 'gen is incorrect (t4)');
	perform test.pathman_equal(res.v2::text, 'k = 1', 'v2 is incorrect (t4)');

	return 'ok';
end;
$$ language plpgsql
set enable_hashjoin = off
set enable_mergejoin = off;



create table test.run_values as select generate_series(1, 10000) val;
create table test.runtime_test_1(id serial primary key, val real);
insert into test.runtime_test_1 select generate_series(1, 10000), random();
select pathman.create_hash_partitions('test.runtime_test_1', 'id', 6);

create table test.category as (select id, 'cat' || id::text as name from generate_series(1, 4) id);
create table test.runtime_test_2 (id serial, category_id int not null, name text, rating real);
insert into test.runtime_test_2 (select id, (id % 6) + 1 as category_id, 'good' || id::text as name, random() as rating from generate_series(1, 100000) id);
create index on test.runtime_test_2 (category_id, rating);
select pathman.create_hash_partitions('test.runtime_test_2', 'category_id', 6);

create table test.vals as (select generate_series(1, 10000) as val);
create table test.runtime_test_3(val text, id serial not null);
insert into test.runtime_test_3(id, val) select * from generate_series(1, 10000) k, format('k = %s', k);
select pathman.create_hash_partitions('test.runtime_test_3', 'id', 4);
create index on test.runtime_test_3 (id);
create index on test.runtime_test_3_0 (id);

create table test.runtime_test_4(val text, id int not null);
insert into test.runtime_test_4(id, val) select * from generate_series(1, 10000) k, md5(k::text);
select pathman.create_range_partitions('test.runtime_test_4', 'id', 1, 2000);


VACUUM ANALYZE;


set pg_pathman.enable_runtimeappend = on;
set pg_pathman.enable_runtimemergeappend = on;

select test.pathman_test_1(); /* RuntimeAppend (select ... where id = (subquery)) */
select test.pathman_test_2(); /* RuntimeAppend (select ... where id = any(subquery)) */
select test.pathman_test_3(); /* RuntimeAppend (a join b on a.id = b.val) */
select test.pathman_test_4(); /* RuntimeMergeAppend (lateral) */
select test.pathman_test_5(); /* projection tests for RuntimeXXX nodes */


/* RuntimeAppend (join, enabled parent) */
select pathman.set_enable_parent('test.runtime_test_1', true);

explain (costs off)
select from test.runtime_test_1 as t1
join (select * from test.run_values limit 4) as t2 on t1.id = t2.val;

select from test.runtime_test_1 as t1
join (select * from test.run_values limit 4) as t2 on t1.id = t2.val;

/* RuntimeAppend (join, disabled parent) */
select pathman.set_enable_parent('test.runtime_test_1', false);

explain (costs off)
select from test.runtime_test_1 as t1
join (select * from test.run_values limit 4) as t2 on t1.id = t2.val;

select from test.runtime_test_1 as t1
join (select * from test.run_values limit 4) as t2 on t1.id = t2.val;

/* RuntimeAppend (join, additional projections) */
select generate_series(1, 2) from test.runtime_test_1 as t1
join (select * from test.run_values limit 4) as t2 on t1.id = t2.val;

/* RuntimeAppend (select ... where id = ANY (subquery), missing partitions) */
select count(*) = 0 from pathman.pathman_partition_list
where parent = 'test.runtime_test_4'::regclass and coalesce(range_min::int, 1) < 0;

/* RuntimeAppend (check that dropped columns don't break tlists) */
create table test.dropped_cols(val int4 not null);
select pathman.create_hash_partitions('test.dropped_cols', 'val', 4);
insert into test.dropped_cols select generate_series(1, 100);
alter table test.dropped_cols add column new_col text;	/* add column */
alter table test.dropped_cols drop column new_col;		/* drop column! */
explain (costs off) select * from generate_series(1, 10) f(id), lateral (select count(1) FILTER (WHERE true) from test.dropped_cols where val = f.id) c;
drop table test.dropped_cols cascade;

set enable_hashjoin = off;
set enable_mergejoin = off;

select from test.runtime_test_4
where id = any (select generate_series(-10, -1)); /* should be empty */

set enable_hashjoin = on;
set enable_mergejoin = on;


DROP TABLE test.vals CASCADE;
DROP TABLE test.category CASCADE;
DROP TABLE test.run_values CASCADE;
DROP TABLE test.runtime_test_1 CASCADE;
DROP TABLE test.runtime_test_2 CASCADE;
DROP TABLE test.runtime_test_3 CASCADE;
DROP TABLE test.runtime_test_4 CASCADE;
DROP FUNCTION test.pathman_assert(bool, text);
DROP FUNCTION test.pathman_equal(text, text, text);
DROP FUNCTION test.pathman_test(text);
DROP FUNCTION test.pathman_test_1();
DROP FUNCTION test.pathman_test_2();
DROP FUNCTION test.pathman_test_3();
DROP FUNCTION test.pathman_test_4();
DROP FUNCTION test.pathman_test_5();
DROP SCHEMA test;
--
--
-- PGPRO-7928
-- Variable pg_pathman.enable must be called before any query.
--
CREATE TABLE part_test (val int NOT NULL);
SELECT pathman.create_hash_partitions('part_test', 'val', 2, partition_names := array['part_test_1','pg_pathman']);
CREATE OR REPLACE FUNCTION part_test_trigger() RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE '%', format('%s %s %s (%s)', TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME);
	IF TG_OP::text = 'DELETE'::text then
		SET pg_pathman.enable = f;
		RETURN new;
	END IF;
END;
$$ LANGUAGE PLPGSQL;
SET pg_pathman.enable_partitionrouter = t;
CREATE TRIGGER ad AFTER DELETE ON part_test_1 FOR EACH ROW EXECUTE PROCEDURE part_test_trigger ();
INSERT INTO part_test VALUES (1);
UPDATE part_test SET val = val + 1 RETURNING *, tableoid::regclass;
UPDATE part_test SET val = val + 1 RETURNING *, tableoid::regclass;

RESET pg_pathman.enable_partitionrouter;
DROP TABLE part_test CASCADE;
DROP FUNCTION part_test_trigger();

DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman;
