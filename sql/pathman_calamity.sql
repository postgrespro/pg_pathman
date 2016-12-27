\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA calamity;


/* call for coverage test */
set client_min_messages = ERROR;
SELECT debug_capture();
set client_min_messages = NOTICE;


/* create table to be partitioned */
CREATE TABLE calamity.part_test(val serial);


/* check function build_hash_condition() */
SELECT build_hash_condition('int4', 'val', 10, 1);
SELECT build_hash_condition('text', 'val', 10, 1);
SELECT build_hash_condition('int4', 'val', 1, 1);
SELECT build_hash_condition('int4', 'val', 10, 20);
SELECT build_hash_condition('text', 'val', 10, NULL) IS NULL;
SELECT build_hash_condition('calamity.part_test', 'val', 10, 1);

/* check function build_range_condition() */
SELECT build_range_condition('val', 10, 20);
SELECT build_range_condition('val', 10, NULL) IS NULL;

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

/* check function get_attribute_type() */
SELECT get_attribute_type('calamity.part_test', 'val');
SELECT get_attribute_type('calamity.part_test', NULL) IS NULL;
SELECT get_attribute_type(NULL, 'val') IS NULL;
SELECT get_attribute_type(NULL, NULL) IS NULL;

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

/* check invoke_on_partition_created_callback() for RANGE */
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 1, NULL, NULL::int);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 1, 1, NULL);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 1, NULL, 1);

/* check invoke_on_partition_created_callback() for HASH */
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', NULL);
SELECT invoke_on_partition_created_callback('calamity.part_test', 'calamity.part_test', 0);
SELECT invoke_on_partition_created_callback(NULL, 'calamity.part_test', 1);
SELECT invoke_on_partition_created_callback('calamity.part_test', NULL, 1);

/* check function add_to_pathman_config() -- PHASE #1 */
SELECT add_to_pathman_config('calamity.part_test', NULL);
SELECT add_to_pathman_config('calamity.part_test', 'val');
SELECT disable_pathman_for('calamity.part_test');
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
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
CHECK (val < 10); /* wrong constraint */
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */
ALTER TABLE calamity.wrong_partition DROP CONSTRAINT pathman_wrong_partition_1_check;

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
