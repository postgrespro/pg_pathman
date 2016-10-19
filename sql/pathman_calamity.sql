\set VERBOSITY terse

CREATE EXTENSION pg_pathman;
CREATE SCHEMA calamity;


/* call for coverage test */
set client_min_messages = ERROR;
SELECT debug_capture();
set client_min_messages = NOTICE;


/* create table to be partitioned */
CREATE TABLE calamity.part_test(val serial);


/* check function validate_relname() */
SELECT validate_relname('calamity.part_test');
/* SELECT validate_relname(NULL); -- FIXME: %s */

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


DROP SCHEMA calamity CASCADE;
DROP EXTENSION pg_pathman;
