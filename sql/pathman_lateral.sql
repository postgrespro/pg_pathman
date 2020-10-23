/*
 * Sometimes join selectivity improvements patches in pgpro force nested loop
 * members swap -- in pathman_lateral_1.out and pathman_lateral_3.out
 *
 * Since 55a1954da16 and 6ef77cf46e8 (>= 13) output of EXPLAIN was changed,
 * now it includes aliases for inherited tables.
 */


\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_lateral;


/* create table partitioned by HASH */
create table test_lateral.data(id int8 not null);
select create_hash_partitions('test_lateral.data', 'id', 10);
insert into test_lateral.data select generate_series(1, 10000);


VACUUM ANALYZE;


set enable_hashjoin = off;
set enable_mergejoin = off;


/* all credits go to Ivan Frolkov */
explain (costs off)
select * from
	test_lateral.data as t1,
	lateral(select * from test_lateral.data as t2 where t2.id > t1.id) t2,
	lateral(select * from test_lateral.data as t3 where t3.id = t2.id + t1.id) t3
			where t1.id between 1 and 100 and
				  t2.id between 2 and 299 and
				  t1.id > t2.id and
				  exists(select * from test_lateral.data t
						 where t1.id = t2.id and t.id = t3.id);


set enable_hashjoin = on;
set enable_mergejoin = on;



DROP SCHEMA test_lateral CASCADE;
DROP EXTENSION pg_pathman;
