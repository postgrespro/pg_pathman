\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA calamity;


/* call for coverage test */
set client_min_messages = ERROR;
SELECT debug_capture();
SELECT get_pathman_lib_version();
set client_min_messages = NOTICE;


/* create table to be partitioned */
CREATE TABLE calamity.part_test(val serial);


/* test pg_pathman's cache */
INSERT INTO calamity.part_test SELECT generate_series(1, 30);

SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
SELECT drop_partitions('calamity.part_test');
SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
SELECT drop_partitions('calamity.part_test');

SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
SELECT append_range_partition('calamity.part_test');
SELECT drop_partitions('calamity.part_test');

SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
SELECT append_range_partition('calamity.part_test');
SELECT drop_partitions('calamity.part_test');

SELECT count(*) FROM calamity.part_test;

DELETE FROM calamity.part_test;


/* test function create_hash_partitions() */
SELECT create_hash_partitions('calamity.part_test', 'val', 2,
							  partition_names := ARRAY[]::TEXT[]); /* not ok */

SELECT create_hash_partitions('calamity.part_test', 'val', 2,
							  partition_names := ARRAY[ 'p1', NULL ]::TEXT[]); /* not ok */

SELECT create_hash_partitions('calamity.part_test', 'val', 2,
							  partition_names := ARRAY[ ['p1'], ['p2'] ]::TEXT[]); /* not ok */

SELECT create_hash_partitions('calamity.part_test', 'val', 2,
							  partition_names := ARRAY['calamity.p1']::TEXT[]); /* not ok */

SELECT create_hash_partitions('calamity.part_test', 'val', 2,
							  tablespaces := ARRAY['abcd']::TEXT[]); /* not ok */


/* test case when naming sequence does not exist */
CREATE TABLE calamity.no_naming_seq(val INT4 NOT NULL);
SELECT add_to_pathman_config('calamity.no_naming_seq', 'val', '100');
select add_range_partition(' calamity.no_naming_seq', 10, 20);
DROP TABLE calamity.no_naming_seq CASCADE;


/* test (-inf, +inf) partition creation */
CREATE TABLE calamity.double_inf(val INT4 NOT NULL);
SELECT add_to_pathman_config('calamity.double_inf', 'val', '10');
select add_range_partition('calamity.double_inf', NULL::INT4, NULL::INT4,
						   partition_name := 'double_inf_part');
DROP TABLE calamity.double_inf CASCADE;


/* test stub 'enable_parent' value for PATHMAN_CONFIG_PARAMS */
INSERT INTO calamity.part_test SELECT generate_series(1, 30);
SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
DELETE FROM pathman_config_params WHERE partrel = 'calamity.part_test'::regclass;
SELECT append_range_partition('calamity.part_test');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_test;
SELECT drop_partitions('calamity.part_test', true);
DELETE FROM calamity.part_test;


/* check function validate_interval_value() */
SELECT set_interval('pg_catalog.pg_class', 100); /* not ok */

INSERT INTO calamity.part_test SELECT generate_series(1, 30);
SELECT create_range_partitions('calamity.part_test', 'val', 1, 10);
SELECT set_interval('calamity.part_test', 100);				/* ok */
SELECT set_interval('calamity.part_test', 15.6);			/* not ok */
SELECT set_interval('calamity.part_test', 'abc'::text);		/* not ok */
SELECT drop_partitions('calamity.part_test', true);
DELETE FROM calamity.part_test;


/* check function build_hash_condition() */
SELECT build_hash_condition('int4', 'val', 10, 1);
SELECT build_hash_condition('text', 'val', 10, 1);
SELECT build_hash_condition('int4', 'val', 1, 1);
SELECT build_hash_condition('int4', 'val', 10, 20);
SELECT build_hash_condition('text', 'val', 10, NULL) IS NULL;
SELECT build_hash_condition('calamity.part_test', 'val', 10, 1);

/* check function build_range_condition() */
SELECT build_range_condition('calamity.part_test', 'val', 10, 20);
SELECT build_range_condition('calamity.part_test', 'val', 10, NULL);
SELECT build_range_condition('calamity.part_test', 'val', NULL, 10);

/* check function validate_relname() */
SELECT validate_relname('calamity.part_test');
SELECT validate_relname(1::REGCLASS);
SELECT validate_relname(NULL);

/* check function get_number_of_partitions() */
SELECT get_number_of_partitions('calamity.part_test');
SELECT get_number_of_partitions(NULL) IS NULL;

/* check function get_parent_of_partition() */
SELECT get_parent_of_partition('calamity.part_test');
SELECT get_parent_of_partition(NULL) IS NULL;

/* check function get_base_type() */
CREATE DOMAIN calamity.test_domain AS INT4;
SELECT get_base_type('int4'::regtype);
SELECT get_base_type('calamity.test_domain'::regtype);
SELECT get_base_type(NULL) IS NULL;

/* check function get_partition_key_type() */
SELECT get_partition_key_type('calamity.part_test');
SELECT get_partition_key_type(0::regclass);
SELECT get_partition_key_type(NULL) IS NULL;

/* check function build_check_constraint_name_attnum() */
SELECT build_check_constraint_name('calamity.part_test', 1::int2);
SELECT build_check_constraint_name('calamity.part_test', NULL::int2) IS NULL;
SELECT build_check_constraint_name(NULL, 1::int2) IS NULL;
SELECT build_check_constraint_name(NULL, NULL::int2) IS NULL;

/* check function build_check_constraint_name_attname() */
SELECT build_check_constraint_name('calamity.part_test', 'val');
SELECT build_check_constraint_name('calamity.part_test', NULL::text) IS NULL;
SELECT build_check_constraint_name(NULL, 'val') IS NULL;
SELECT build_check_constraint_name(NULL, NULL::text) IS NULL;

/* check function build_update_trigger_name() */
SELECT build_update_trigger_name('calamity.part_test');
SELECT build_update_trigger_name(NULL) IS NULL;

/* check function build_update_trigger_func_name() */
SELECT build_update_trigger_func_name('calamity.part_test');
SELECT build_update_trigger_func_name(NULL) IS NULL;

/* check function stop_concurrent_part_task() */
SELECT stop_concurrent_part_task(1::regclass);

/* check function drop_range_partition_expand_next() */
SELECT drop_range_partition_expand_next('pg_class');
SELECT drop_range_partition_expand_next(NULL) IS NULL;


/* check invoke_on_partition_created_callback() */
CREATE FUNCTION calamity.dummy_cb(arg jsonb) RETURNS void AS $$
	begin
		raise warning 'arg: %', arg::text;
	end
$$ LANGUAGE plpgsql;

/* Invalid args */
SELECT invoke_on_partition_created_callback(NULL, 'calamity.part_test', 1);
SELECT invoke_on_partition_created_callback('calamity.part_test', NULL, 1);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 0);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 1);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', NULL);

/* HASH */
SELECT invoke_on_partition_created_callback(0::regclass, 1::regclass, 'calamity.dummy_cb(jsonb)'::regprocedure);

/* RANGE */
SELECT invoke_on_partition_created_callback('calamity.part_test'::regclass, 'pg_class'::regclass, 'calamity.dummy_cb(jsonb)'::regprocedure, NULL::int, NULL);
SELECT invoke_on_partition_created_callback(0::regclass, 1::regclass, 'calamity.dummy_cb(jsonb)'::regprocedure, NULL::int, NULL);
SELECT invoke_on_partition_created_callback(0::regclass, 1::regclass, 'calamity.dummy_cb(jsonb)'::regprocedure, 1, NULL);
SELECT invoke_on_partition_created_callback(0::regclass, 1::regclass, 'calamity.dummy_cb(jsonb)'::regprocedure, NULL, 1);

DROP FUNCTION calamity.dummy_cb(arg jsonb);


/* check function add_to_pathman_config() -- PHASE #1 */
SELECT add_to_pathman_config(NULL, 'val');						/* no table */
SELECT add_to_pathman_config('calamity.part_test', NULL);		/* no column */
SELECT add_to_pathman_config('calamity.part_test', 'V_A_L');	/* wrong column */
SELECT add_to_pathman_config('calamity.part_test', 'val');		/* OK */
SELECT disable_pathman_for('calamity.part_test');
SELECT add_to_pathman_config('calamity.part_test', 'val', '10'); /* OK */
SELECT disable_pathman_for('calamity.part_test');


/* check function add_to_pathman_config() -- PHASE #2 */
CREATE TABLE calamity.part_ok(val serial);
INSERT INTO calamity.part_ok SELECT generate_series(1, 2);
SELECT create_hash_partitions('calamity.part_ok', 'val', 4);
CREATE TABLE calamity.wrong_partition (LIKE calamity.part_test) INHERITS (calamity.part_test); /* wrong partition w\o constraints */

SELECT add_to_pathman_config('calamity.part_test', 'val');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */

SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */

ALTER TABLE calamity.wrong_partition
ADD CONSTRAINT pathman_wrong_partition_1_check
CHECK (val = 1 OR val = 2); /* wrong constraint */
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */
ALTER TABLE calamity.wrong_partition DROP CONSTRAINT pathman_wrong_partition_1_check;

ALTER TABLE calamity.wrong_partition
ADD CONSTRAINT pathman_wrong_partition_1_check
CHECK (val >= 10 AND val = 2); /* wrong constraint */
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */
ALTER TABLE calamity.wrong_partition DROP CONSTRAINT pathman_wrong_partition_1_check;

/* check GUC variable */
SHOW pg_pathman.enable;

/* check function create_hash_partitions_internal() (called for the 2nd time) */
CREATE TABLE calamity.hash_two_times(val serial);
SELECT create_hash_partitions_internal('calamity.hash_two_times', 'val', 2);
SELECT create_hash_partitions('calamity.hash_two_times', 'val', 2);
SELECT create_hash_partitions_internal('calamity.hash_two_times', 'val', 2);

/* check function disable_pathman_for() */
CREATE TABLE calamity.to_be_disabled(val INT NOT NULL);
SELECT create_hash_partitions('calamity.to_be_disabled', 'val', 3);	/* add row to main config */
SELECT set_enable_parent('calamity.to_be_disabled', true); /* add row to params */
SELECT disable_pathman_for('calamity.to_be_disabled'); /* should delete both rows */
SELECT count(*) FROM pathman_config WHERE partrel = 'calamity.to_be_disabled'::REGCLASS;
SELECT count(*) FROM pathman_config_params WHERE partrel = 'calamity.to_be_disabled'::REGCLASS;


DROP SCHEMA calamity CASCADE;
DROP EXTENSION pg_pathman;
