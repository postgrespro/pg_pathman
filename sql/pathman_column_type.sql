/*
 * In 9ce77d75c5a (>= 13) struct Var was changed, which caused the output
 * of get_partition_cooked_key to change.
 */

\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_column_type;


/*
 * RANGE partitioning.
 */

/* create new table (val int) */
CREATE TABLE test_column_type.test(val INT4 NOT NULL);
SELECT create_range_partitions('test_column_type.test', 'val', 1, 10, 10);

/* make sure that bounds and dispatch info has been cached */
SELECT * FROM test_column_type.test;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;

/*
 * Get parsed and analyzed expression.
 */
CREATE FUNCTION get_cached_partition_cooked_key(REGCLASS)
RETURNS TEXT AS 'pg_pathman', 'get_cached_partition_cooked_key_pl'
LANGUAGE C STRICT;

SELECT get_partition_cooked_key('test_column_type.test'::REGCLASS);
SELECT get_cached_partition_cooked_key('test_column_type.test'::REGCLASS);
SELECT get_partition_key_type('test_column_type.test'::REGCLASS);

/* change column's type (should also flush caches) */
ALTER TABLE test_column_type.test ALTER val TYPE NUMERIC;

/* check that correct expression has been built */
SELECT get_partition_key_type('test_column_type.test'::REGCLASS);
SELECT get_partition_cooked_key('test_column_type.test'::REGCLASS);
SELECT get_cached_partition_cooked_key('test_column_type.test'::REGCLASS);
DROP FUNCTION get_cached_partition_cooked_key(REGCLASS);

/* make sure that everything works properly */
SELECT * FROM test_column_type.test;

SELECT context, entries FROM pathman_cache_stats ORDER BY context;

/* check insert dispatching */
INSERT INTO test_column_type.test VALUES (1);
SELECT tableoid::regclass, * FROM test_column_type.test;

SELECT drop_partitions('test_column_type.test');
DROP TABLE test_column_type.test CASCADE;


/*
 * HASH partitioning.
 */

/* create new table (id int, val int) */
CREATE TABLE test_column_type.test(id INT4 NOT NULL, val INT4);
SELECT create_hash_partitions('test_column_type.test', 'id', 5);

/* make sure that bounds and dispatch info has been cached */
SELECT * FROM test_column_type.test;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;

/* change column's type (should NOT work) */
ALTER TABLE test_column_type.test ALTER id TYPE NUMERIC;

/* make sure that everything works properly */
SELECT * FROM test_column_type.test;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;

/* change column's type (should flush caches) */
ALTER TABLE test_column_type.test ALTER val TYPE NUMERIC;

/* make sure that everything works properly */
SELECT * FROM test_column_type.test;
SELECT context, entries FROM pathman_cache_stats ORDER BY context;

/* check insert dispatching */
INSERT INTO test_column_type.test VALUES (1);
SELECT tableoid::regclass, * FROM test_column_type.test;

SELECT drop_partitions('test_column_type.test');
DROP TABLE test_column_type.test CASCADE;


DROP SCHEMA test_column_type CASCADE;
DROP EXTENSION pg_pathman;
