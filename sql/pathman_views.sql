/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on 9.5
 * -------------------------------------------
 *
 * Also since 8edd0e794 (>= 12) Append nodes with single subplan are eliminated,
 * causing different output; pathman_views_2.out is the updated version.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA views;



/* create a partitioned table */
create table views._abc(id int4 not null);
select create_hash_partitions('views._abc', 'id', 10);
insert into views._abc select generate_series(1, 100);

/* create a dummy table */
create table views._abc_add (like views._abc);


vacuum analyze;


/* create a facade view */
create view views.abc as select * from views._abc;

create or replace function views.disable_modification()
returns trigger as
$$
BEGIN
  RAISE EXCEPTION '%', TG_OP;
  RETURN NULL;
END;
$$
language 'plpgsql';

create trigger abc_mod_tr
instead of insert or update or delete
on views.abc for each row
execute procedure views.disable_modification();


/* Test SELECT */
explain (costs off) select * from views.abc;
explain (costs off) select * from views.abc where id = 1;
explain (costs off) select * from views.abc where id = 1 for update;
select * from views.abc where id = 1 for update;
select count (*) from views.abc;


/* Test INSERT */
explain (costs off) insert into views.abc values (1);
insert into views.abc values (1);


/* Test UPDATE */
explain (costs off) update views.abc set id = 2 where id = 1 or id = 2;
update views.abc set id = 2 where id = 1 or id = 2;


/* Test DELETE */
explain (costs off) delete from views.abc where id = 1 or id = 2;
delete from views.abc where id = 1 or id = 2;


/* Test SELECT with UNION */
create view views.abc_union as table views._abc union table views._abc_add;
create view views.abc_union_all as table views._abc union all table views._abc_add;
explain (costs off) table views.abc_union;
explain (costs off) select * from views.abc_union where id = 5;
explain (costs off) table views.abc_union_all;
explain (costs off) select * from views.abc_union_all where id = 5;



DROP SCHEMA views CASCADE;
DROP EXTENSION pg_pathman;
