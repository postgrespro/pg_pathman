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

/* change column's type (should flush caches) */
ALTER TABLE test_column_type.test ALTER val TYPE NUMERIC;

/* check that parsed expression was cleared */
SELECT partrel, expression_p FROM pathman_config;

/* make sure that everything works properly */
SELECT * FROM test_column_type.test;

/* check that expression has been built */
SELECT partrel, expression_p FROM pathman_config;

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
