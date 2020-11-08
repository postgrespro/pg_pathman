/*
 * pathman_calamity.out and pathman_calamity_1.out differ only in that since
 * 12 we get
 *   ERROR:  invalid input syntax for type integer: "abc"
 * instead of
 *   ERROR:  invalid input syntax for integer: "15.6"
 *
 * Since 55a1954da16 and 6ef77cf46e8 (>= 13) output of EXPLAIN was changed,
 * now it includes aliases for inherited tables.
 */

\set VERBOSITY terse
SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA calamity;


/* call for coverage test */
set client_min_messages = ERROR;
SELECT debug_capture();
SELECT pathman_version();
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


/* test function create_single_range_partition() */
SELECT create_single_range_partition(NULL, NULL::INT4, NULL);					/* not ok */
SELECT create_single_range_partition('pg_class', NULL::INT4, NULL);				/* not ok */

SELECT add_to_pathman_config('calamity.part_test', 'val');
SELECT create_single_range_partition('calamity.part_test', NULL::INT4, NULL);	/* not ok */
DELETE FROM pathman_config WHERE partrel = 'calamity.part_test'::REGCLASS;


/* test function create_range_partitions_internal() */
SELECT create_range_partitions_internal(NULL, '{}'::INT[], NULL, NULL);		/* not ok */

SELECT create_range_partitions_internal('calamity.part_test',
										NULL::INT[], NULL, NULL);			/* not ok */

SELECT create_range_partitions_internal('calamity.part_test', '{1}'::INT[],
										'{part_1}'::TEXT[], NULL);			/* not ok */

SELECT create_range_partitions_internal('calamity.part_test', '{1}'::INT[],
										NULL, '{tblspc_1}'::TEXT[]);		/* not ok */

SELECT create_range_partitions_internal('calamity.part_test',
										'{1, NULL}'::INT[], NULL, NULL);	/* not ok */

SELECT create_range_partitions_internal('calamity.part_test',
										'{2, 1}'::INT[], NULL, NULL);		/* not ok */

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
SELECT build_range_condition(NULL, 'val', 10, 20);						/* not ok */
SELECT build_range_condition('calamity.part_test', NULL, 10, 20);		/* not ok */
SELECT build_range_condition('calamity.part_test', 'val', 10, 20);		/* OK */
SELECT build_range_condition('calamity.part_test', 'val', 10, NULL);	/* OK */
SELECT build_range_condition('calamity.part_test', 'val', NULL, 10);	/* OK */

/* check function validate_interval_value() */
SELECT validate_interval_value(1::REGCLASS, 'expr', 2, '1 mon');		/* not ok */
SELECT validate_interval_value(NULL, 'expr', 2, '1 mon');				/* not ok */
SELECT validate_interval_value('pg_class', NULL, 2, '1 mon');			/* not ok */
SELECT validate_interval_value('pg_class', 'relname', NULL, '1 mon');	/* not ok */
SELECT validate_interval_value('pg_class', 'relname', 1, 'HASH');		/* not ok */
SELECT validate_interval_value('pg_class', 'expr', 2, '1 mon');			/* not ok */
SELECT validate_interval_value('pg_class', 'expr', 2, NULL);			/* not ok */
SELECT validate_interval_value('pg_class', 'EXPR', 1, 'HASH');			/* not ok */

/* check function validate_relname() */
SELECT validate_relname('calamity.part_test');
SELECT validate_relname(1::REGCLASS);
SELECT validate_relname(NULL);

/* check function validate_expression() */
SELECT validate_expression(1::regclass, NULL);					/* not ok */
SELECT validate_expression(NULL::regclass, NULL);				/* not ok */
SELECT validate_expression('calamity.part_test', NULL);			/* not ok */
SELECT validate_expression('calamity.part_test', 'valval');		/* not ok */
SELECT validate_expression('calamity.part_test', 'random()');	/* not ok */
SELECT validate_expression('calamity.part_test', 'val');		/* OK */
SELECT validate_expression('calamity.part_test', 'VaL');		/* OK */

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

/* check function build_check_constraint_name() */
SELECT build_check_constraint_name('calamity.part_test');		/* OK */
SELECT build_check_constraint_name(0::REGCLASS);				/* not ok */
SELECT build_check_constraint_name(NULL) IS NULL;

/* check function build_sequence_name() */
SELECT build_sequence_name('calamity.part_test');				/* OK */
SELECT build_sequence_name(1::REGCLASS);						/* not ok */
SELECT build_sequence_name(NULL) IS NULL;

/* check function partition_table_concurrently() */
SELECT partition_table_concurrently(1::REGCLASS);				/* not ok */
SELECT partition_table_concurrently('pg_class', 0);				/* not ok */
SELECT partition_table_concurrently('pg_class', 1, 1E-5);		/* not ok */
SELECT partition_table_concurrently('pg_class');				/* not ok */

/* check function stop_concurrent_part_task() */
SELECT stop_concurrent_part_task(1::REGCLASS);					/* not ok */

/* check function drop_range_partition_expand_next() */
SELECT drop_range_partition_expand_next('pg_class');			/* not ok */
SELECT drop_range_partition_expand_next(NULL) IS NULL;

/* check function generate_range_bounds() */
SELECT generate_range_bounds(NULL, 100, 10) IS NULL;
SELECT generate_range_bounds(0, NULL::INT4, 10) IS NULL;
SELECT generate_range_bounds(0, 100, NULL) IS NULL;
SELECT generate_range_bounds(0, 100, 0);							/* not ok */
SELECT generate_range_bounds('a'::TEXT, 'test'::TEXT, 10);			/* not ok */
SELECT generate_range_bounds('a'::TEXT, '1 mon'::INTERVAL, 10);		/* not ok */
SELECT generate_range_bounds(0::NUMERIC, 1::NUMERIC, 10);			/* OK */
SELECT generate_range_bounds('1-jan-2017'::DATE,
							 '1 day'::INTERVAL,
							 4);									/* OK */

SELECT check_range_available(NULL, NULL::INT4, NULL);	/* not ok */
SELECT check_range_available('pg_class', 1, 10);		/* OK (not partitioned) */

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
SELECT add_to_pathman_config(0::REGCLASS, 'val');				/* no table (oid) */
SELECT add_to_pathman_config('calamity.part_test', NULL);		/* no expr */
SELECT add_to_pathman_config('calamity.part_test', 'V_A_L');	/* wrong expr */
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
ADD CONSTRAINT pathman_wrong_partition_check
CHECK (val = 1 OR val = 2); /* wrong constraint */
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */
ALTER TABLE calamity.wrong_partition DROP CONSTRAINT pathman_wrong_partition_check;

ALTER TABLE calamity.wrong_partition
ADD CONSTRAINT pathman_wrong_partition_check
CHECK (val >= 10 AND val = 2); /* wrong constraint */
SELECT add_to_pathman_config('calamity.part_test', 'val', '10');
EXPLAIN (COSTS OFF) SELECT * FROM calamity.part_ok; /* check that pathman is enabled */
ALTER TABLE calamity.wrong_partition DROP CONSTRAINT pathman_wrong_partition_check;

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


/* check function get_part_range_by_idx() */
CREATE TABLE calamity.test_range_idx(val INT4 NOT NULL);
SELECT create_range_partitions('calamity.test_range_idx', 'val', 1, 10, 1);

SELECT get_part_range(NULL, 1, NULL::INT4);							/* not ok */
SELECT get_part_range('calamity.test_range_idx', NULL, NULL::INT4);	/* not ok */
SELECT get_part_range('calamity.test_range_idx', 0, NULL::INT2);	/* not ok */
SELECT get_part_range('calamity.test_range_idx', -2, NULL::INT4);	/* not ok */
SELECT get_part_range('calamity.test_range_idx', 4, NULL::INT4);	/* not ok */
SELECT get_part_range('calamity.test_range_idx', 0, NULL::INT4);	/* OK */

DROP TABLE calamity.test_range_idx CASCADE;


/* check function get_part_range_by_oid() */
CREATE TABLE calamity.test_range_oid(val INT4 NOT NULL);
SELECT create_range_partitions('calamity.test_range_oid', 'val', 1, 10, 1);

SELECT get_part_range(NULL, NULL::INT4);							/* not ok */
SELECT get_part_range('pg_class', NULL::INT4);						/* not ok */
SELECT get_part_range('calamity.test_range_oid_1', NULL::INT2);		/* not ok */
SELECT get_part_range('calamity.test_range_oid_1', NULL::INT4);		/* OK */

DROP TABLE calamity.test_range_oid CASCADE;


/* check function merge_range_partitions() */
SELECT merge_range_partitions('pg_class');							/* not ok */
SELECT merge_range_partitions('pg_class', 'pg_inherits');			/* not ok */

CREATE TABLE calamity.merge_test_a(val INT4 NOT NULL);
CREATE TABLE calamity.merge_test_b(val INT4 NOT NULL);

SELECT create_range_partitions('calamity.merge_test_a', 'val', 1, 10, 2);
SELECT create_range_partitions('calamity.merge_test_b', 'val', 1, 10, 2);

SELECT merge_range_partitions('calamity.merge_test_a_1',
							  'calamity.merge_test_b_1');			/* not ok */

DROP TABLE calamity.merge_test_a,calamity.merge_test_b CASCADE;

DROP SCHEMA calamity CASCADE;
DROP EXTENSION pg_pathman;



/*
 * -------------------------------
 *  Special tests (SET statement)
 * -------------------------------
 */

CREATE EXTENSION pg_pathman;

SET pg_pathman.enable = false;
SET pg_pathman.enable = true;
SET pg_pathman.enable = false;
RESET pg_pathman.enable;
RESET ALL;
BEGIN; ROLLBACK;
BEGIN ISOLATION LEVEL SERIALIZABLE; ROLLBACK;
BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; ROLLBACK;

DROP EXTENSION pg_pathman;



/*
 * -------------------------------------
 *  Special tests (pathman_cache_stats)
 * -------------------------------------
 */

CREATE SCHEMA calamity;
CREATE EXTENSION pg_pathman;

/* check that cache loading is lazy */
CREATE TABLE calamity.test_pathman_cache_stats(val NUMERIC NOT NULL);
SELECT create_range_partitions('calamity.test_pathman_cache_stats', 'val', 1, 10, 10);
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */
DROP TABLE calamity.test_pathman_cache_stats CASCADE;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */

/* Change this setting for code coverage */
SET pg_pathman.enable_bounds_cache = false;

/* check view pathman_cache_stats (bounds cache disabled) */
CREATE TABLE calamity.test_pathman_cache_stats(val NUMERIC NOT NULL);
SELECT create_range_partitions('calamity.test_pathman_cache_stats', 'val', 1, 10, 10);
EXPLAIN (COSTS OFF) SELECT * FROM calamity.test_pathman_cache_stats;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */
DROP TABLE calamity.test_pathman_cache_stats CASCADE;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */

/* Restore this GUC */
SET pg_pathman.enable_bounds_cache = true;

/* check view pathman_cache_stats (bounds cache enabled) */
CREATE TABLE calamity.test_pathman_cache_stats(val NUMERIC NOT NULL);
SELECT create_range_partitions('calamity.test_pathman_cache_stats', 'val', 1, 10, 10);
EXPLAIN (COSTS OFF) SELECT * FROM calamity.test_pathman_cache_stats;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */
DROP TABLE calamity.test_pathman_cache_stats CASCADE;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */

/* check that parents cache has been flushed after partition was dropped */
CREATE TABLE calamity.test_pathman_cache_stats(val NUMERIC NOT NULL);
SELECT create_range_partitions('calamity.test_pathman_cache_stats', 'val', 1, 10, 10);
EXPLAIN (COSTS OFF) SELECT * FROM calamity.test_pathman_cache_stats;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */
SELECT drop_range_partition('calamity.test_pathman_cache_stats_1');
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */
DROP TABLE calamity.test_pathman_cache_stats CASCADE;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;	/* OK */

DROP SCHEMA calamity CASCADE;
DROP EXTENSION pg_pathman;



/*
 * ------------------------------------------
 *  Special tests (uninitialized pg_pathman)
 * ------------------------------------------
 */

CREATE SCHEMA calamity;
CREATE EXTENSION pg_pathman;


/* check function pathman_cache_search_relid() */
CREATE TABLE calamity.survivor(val INT NOT NULL);
SELECT create_range_partitions('calamity.survivor', 'val', 1, 10, 2);

DROP EXTENSION pg_pathman CASCADE;
SET pg_pathman.enable = f; /* DON'T LOAD CONFIG */
CREATE EXTENSION pg_pathman;
SHOW pg_pathman.enable;

SELECT add_to_pathman_config('calamity.survivor', 'val', '10');	/* not ok */
SELECT * FROM pathman_partition_list;							/* not ok */
SELECT get_part_range('calamity.survivor', 0, NULL::INT);		/* not ok */
EXPLAIN (COSTS OFF) SELECT * FROM calamity.survivor;			/* OK */

SET pg_pathman.enable = t; /* LOAD CONFIG */

SELECT add_to_pathman_config('calamity.survivor', 'val', '10');	/* OK */
SELECT * FROM pathman_partition_list;							/* OK */
SELECT get_part_range('calamity.survivor', 0, NULL::INT);		/* OK */
EXPLAIN (COSTS OFF) SELECT * FROM calamity.survivor;			/* OK */

DROP TABLE calamity.survivor CASCADE;


DROP SCHEMA calamity CASCADE;
DROP EXTENSION pg_pathman;
