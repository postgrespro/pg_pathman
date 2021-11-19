\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_update_triggers;



create table test_update_triggers.test (val int not null);
select create_hash_partitions('test_update_triggers.test', 'val', 2,
							  partition_names := array[
								'test_update_triggers.test_1',
								'test_update_triggers.test_2']);


create or replace function test_update_triggers.test_trigger() returns trigger as $$
begin
	raise notice '%', format('%s %s %s (%s)', TG_WHEN, TG_OP, TG_LEVEL, TG_TABLE_NAME);

	if TG_OP::text = 'DELETE'::text then
		return old;
	else
		return new;
	end if; end;
$$ language plpgsql;


/* Enable our precious custom node */
set pg_pathman.enable_partitionrouter = t;


/*
 * Statement level triggers
 */

create trigger bus before update ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();
create trigger bds before delete ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();
create trigger bis before insert ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();


create trigger aus after update ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();
create trigger ads after delete ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();
create trigger ais after insert ON test_update_triggers.test
	execute procedure test_update_triggers.test_trigger ();


create trigger bus before update ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();
create trigger bds before delete ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();
create trigger bis before insert ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();

create trigger aus after update ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();
create trigger ads after delete ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();
create trigger ais after insert ON test_update_triggers.test_1
	execute procedure test_update_triggers.test_trigger ();


create trigger bus before update ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();
create trigger bds before delete ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();
create trigger bis before insert ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();

create trigger aus after update ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();
create trigger ads after delete ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();
create trigger ais after insert ON test_update_triggers.test_2
	execute procedure test_update_triggers.test_trigger ();


/* multiple values */
insert into test_update_triggers.test select generate_series(1, 200);

update test_update_triggers.test set val = val + 1;
update test_update_triggers.test set val = val + 1;
update test_update_triggers.test set val = val + 1;
update test_update_triggers.test set val = val + 1;
update test_update_triggers.test set val = val + 1;

select count(distinct val) from test_update_triggers.test;


truncate test_update_triggers.test;


/*
 * Row level triggers
 */

create trigger bu before update ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger bd before delete ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger bi before insert ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();

create trigger au after update ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger ad after delete ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger ai after insert ON test_update_triggers.test_1
	for each row execute procedure test_update_triggers.test_trigger ();


create trigger bu before update ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger bd before delete ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger bi before insert ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();

create trigger au after update ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger ad after delete ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();
create trigger ai after insert ON test_update_triggers.test_2
	for each row execute procedure test_update_triggers.test_trigger ();


/* single value */
insert into test_update_triggers.test values (1);

update test_update_triggers.test set val = val + 1 returning *, tableoid::regclass;
update test_update_triggers.test set val = val + 1 returning *, tableoid::regclass;
update test_update_triggers.test set val = val + 1 returning *, tableoid::regclass;
update test_update_triggers.test set val = val + 1 returning *, tableoid::regclass;
update test_update_triggers.test set val = val + 1 returning *, tableoid::regclass;

select count(distinct val) from test_update_triggers.test;


DROP TABLE test_update_triggers.test CASCADE;
DROP FUNCTION test_update_triggers.test_trigger();
DROP SCHEMA test_update_triggers;
DROP EXTENSION pg_pathman CASCADE;
