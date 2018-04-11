\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA dropped_cols;


/*
 * we should be able to manage tables with dropped columns
 */

create table test_range(a int, b int, key int not null);

alter table test_range drop column a;
select create_range_partitions('test_range', 'key', 1, 10, 2);

alter table test_range drop column b;
select prepend_range_partition('test_range');

select * from pathman_partition_list order by parent, partition;
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_range_1_check';
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_range_3_check';

drop table test_range cascade;


create table test_hash(a int, b int, key int not null);

alter table test_hash drop column a;
select create_hash_partitions('test_hash', 'key', 3);

alter table test_hash drop column b;
create table test_dummy (like test_hash);
select replace_hash_partition('test_hash_2', 'test_dummy', true);

select * from pathman_partition_list order by parent, partition;
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_hash_1_check';
select pg_get_constraintdef(oid, true) from pg_constraint where conname = 'pathman_test_dummy_check';
drop table test_hash cascade;


DROP SCHEMA dropped_cols CASCADE;
DROP EXTENSION pg_pathman;
