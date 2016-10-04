\set VERBOSITY terse

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
set pg_pathman.enable = true
set enable_mergejoin = off
set enable_hashjoin = off;

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
								   format('"runtime_test_1_%s"', pathman.get_hash_part_idx(hashint4(i + 1), 6)),
								   'wrong partition');

		num = plan->0->'Plan'->'Plans'->1->'Plans'->i->'Actual Loops';
		perform test.pathman_equal(num::text, '1', 'expected 1 loop');
	end loop;

	return 'ok';
end;
$$ language plpgsql
set pg_pathman.enable = true
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
set pg_pathman.enable = true
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
set pg_pathman.enable = true
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


	select id, generate_series(1, 2) gen, val
	from test.runtime_test_3
	where id = any (select * from test.vals order by val limit 5)
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
set pg_pathman.enable = true
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


analyze test.run_values;
analyze test.runtime_test_1;
analyze test.runtime_test_2;
analyze test.runtime_test_3;
analyze test.runtime_test_3_0;

set pg_pathman.enable_runtimeappend = on;
set pg_pathman.enable_runtimemergeappend = on;

select test.pathman_test_1(); /* RuntimeAppend (select ... where id = (subquery)) */
select test.pathman_test_2(); /* RuntimeAppend (select ... where id = any(subquery)) */
select test.pathman_test_3(); /* RuntimeAppend (a join b on a.id = b.val) */
select test.pathman_test_4(); /* RuntimeMergeAppend (lateral) */
select test.pathman_test_5(); /* projection tests for RuntimeXXX nodes */


DROP SCHEMA test CASCADE;
DROP EXTENSION pg_pathman CASCADE;
DROP SCHEMA pathman CASCADE;

