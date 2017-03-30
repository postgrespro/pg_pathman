\set VERBOSITY terse

SET search_path = 'public';
CREATE EXTENSION pg_pathman;
CREATE SCHEMA test_column_type;


/* create new table (val int) */
CREATE TABLE test_column_type.test(val INT4 NOT NULL);
SELECT create_range_partitions('test_column_type.test', 'val', 1, 10, 10);

/* make sure that bounds and dispatch info has been cached */
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


DROP SCHEMA test_column_type CASCADE;
DROP EXTENSION pg_pathman;
